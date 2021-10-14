#define LOG_MEMORY_PERF_COUNTERS

using System;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Extensions.Logging;
using Orleans.Runtime;

namespace Orleans.Statistics
{
    internal class LinuxEnvironmentStatistics : IHostEnvironmentStatistics, ILifecycleParticipant<ISiloLifecycle>, ILifecycleParticipant<IClusterClientLifecycle>, ILifecycleObserver, IDisposable
    {
        private readonly ILogger _logger;

        private const float KB = 1024f;

        /// <inheritdoc />
        public long? TotalPhysicalMemory { get; private set; }

        /// <inheritdoc />
        public float? CpuUsage { get; private set; }

        /// <inheritdoc />
        public long? AvailableMemory { get; private set; }

        /// <inheritdoc />
        public bool InCGroup { get; private set; }

        /// <inheritdoc />
        private long MemoryUsage => GC.GetTotalMemory(false);

        private readonly TimeSpan MONITOR_PERIOD = TimeSpan.FromSeconds(5);

        private CancellationTokenSource _cts;
        private Task _monitorTask;

        private const string MEMINFO_FILEPATH = "/proc/meminfo";
        private const string CPUSTAT_FILEPATH = "/proc/stat";
        private const string CGROUP_V1_FILEPATH = "/sys/fs/cgroup";
        private const string CGROUP_V1_USED_MEM_FILEPATH = "/sys/fs/cgroup/memory/memory.usage_in_bytes";
        private const string CGROUP_V1_TOTAL_MEM_FILEPATH = "/sys/fs/cgroup/memory/memory.limit_in_bytes";
        private const string CGROUP_V1_CFS_QUOTA_FILEPATH = "/sys/fs/cgroup/cpuacct/cpu.cfs_quota_us";
        private const string CGROUP_V1_CFS_PERIOD_FILEPATH = "/sys/fs/cgroup/cpuacct/cpu.cfs_period_us";
        private const string CGROUP_V1_CPU_USAGE_FILEPATH = "/sys/fs/cgroup/cpuacct/cpuacct.usage";

        internal static readonly string[] RequiredFiles = new[]
        {
            MEMINFO_FILEPATH,
            CPUSTAT_FILEPATH,
            CGROUP_V1_FILEPATH,
            CGROUP_V1_USED_MEM_FILEPATH,
            CGROUP_V1_TOTAL_MEM_FILEPATH,
            CGROUP_V1_CFS_QUOTA_FILEPATH,
            CGROUP_V1_CFS_PERIOD_FILEPATH,
            CGROUP_V1_CPU_USAGE_FILEPATH
        };

        public LinuxEnvironmentStatistics(ILoggerFactory loggerFactory)
        {
            _logger = loggerFactory.CreateLogger<LinuxEnvironmentStatistics>();
        }

        public void Dispose()
        {
            if (_cts != null && !_cts.IsCancellationRequested)
            {
                _cts.Cancel();
            }
        }

        public void Participate(ISiloLifecycle lifecycle)
        {
            lifecycle.Subscribe(ServiceLifecycleStage.RuntimeInitialize, this);
        }

        public void Participate(IClusterClientLifecycle lifecycle)
        {
            lifecycle.Subscribe(ServiceLifecycleStage.RuntimeInitialize, this);
        }

        public async Task OnStart(CancellationToken ct)
        {
            _logger.LogTrace($"Starting {nameof(LinuxEnvironmentStatistics)}");

            if (Directory.Exists(CGROUP_V1_FILEPATH))
            {
                _logger.LogTrace($"Process is running inside a CGroup, {nameof(LinuxEnvironmentStatistics)} " +
                                 $"using the CGroup's limits instead of system ones");
               InCGroup = true;
            }

            _cts = new CancellationTokenSource();
            ct.Register(() => _cts.Cancel());

            _monitorTask = await Task.Factory.StartNew(
                () => Monitor(_cts.Token),
                _cts.Token,
                TaskCreationOptions.DenyChildAttach | TaskCreationOptions.RunContinuationsAsynchronously,
                TaskScheduler.Default
            );

            _logger.LogTrace($"Started {nameof(LinuxEnvironmentStatistics)}");
        }

        public async Task OnStop(CancellationToken ct)
        {
            if (_cts == null)
                return;

            _logger.LogTrace($"Stopping {nameof(LinuxEnvironmentStatistics)}");
            try
            {
                _cts.Cancel();
                try
                {
                    await _monitorTask;
                }
                catch (TaskCanceledException) { }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, $"Error stopping {nameof(LinuxEnvironmentStatistics)}");
            }
            finally
            {
                _logger.LogTrace($"Stopped {nameof(LinuxEnvironmentStatistics)}");
            }
        }

        private async Task UpdateTotalPhysicalMemory()
        {
            if (InCGroup)
            {
                var memTotalLine = await ReadLineAsync(CGROUP_V1_TOTAL_MEM_FILEPATH);

                if (string.IsNullOrWhiteSpace(memTotalLine))
                {
                    _logger.LogWarning($"Couldn't read line from '{CGROUP_V1_TOTAL_MEM_FILEPATH}'");
                    return;
                }

                if (!long.TryParse(new string(memTotalLine.Where(char.IsDigit).ToArray()), out var totalMemInB))
                {
                    _logger.LogWarning($"Couldn't parse memory.limit_in_bytes output");
                    return;
                }

                TotalPhysicalMemory = totalMemInB;
            }
            else
            {
                var memTotalLine = await ReadLineStartingWithAsync(MEMINFO_FILEPATH, "MemTotal");

                if (string.IsNullOrWhiteSpace(memTotalLine))
                {
                    _logger.LogWarning($"Couldn't read 'MemTotal' line from '{MEMINFO_FILEPATH}'");
                    return;
                }

                // Format: "MemTotal:       16426476 kB"
                if (!long.TryParse(new string(memTotalLine.Where(char.IsDigit).ToArray()), out var totalMemInKb))
                {
                    _logger.LogWarning($"Couldn't parse meminfo output");
                    return;
                }

                TotalPhysicalMemory = totalMemInKb * 1_000;
            }
        }

        private long _prevIdleTime;
        private long _prevTotalTime;

        private async Task UpdateCpuUsage(int i)
        {
            if (InCGroup)
            {
                var cpuUsageLine = await ReadLineAsync(CGROUP_V1_CPU_USAGE_FILEPATH);
                var cpuUsageQuota = await ReadLineAsync(CGROUP_V1_CFS_QUOTA_FILEPATH);
                var cpuUsagePeriod = await ReadLineAsync(CGROUP_V1_CFS_PERIOD_FILEPATH);

                if (string.IsNullOrWhiteSpace(cpuUsageLine))
                {
                    _logger.LogWarning($"Couldn't read line from '{CGROUP_V1_CPU_USAGE_FILEPATH}'");
                    return;
                }

                if (long.TryParse(cpuUsageLine, out _))
                {
                    _logger.LogWarning($"Failed to parse '{CGROUP_V1_CPU_USAGE_FILEPATH}' output correctly. Line: {cpuUsageLine}");
                    return;
                }

                var cpuUsageNs = long.Parse(cpuUsageLine);

                if (string.IsNullOrWhiteSpace(cpuUsageQuota))
                {
                    _logger.LogWarning($"Couldn't read line from '{CGROUP_V1_CFS_QUOTA_FILEPATH}'");
                    return;
                }

                if (long.TryParse(cpuUsageQuota, out _))
                {
                    _logger.LogWarning($"Failed to parse '{CGROUP_V1_CFS_QUOTA_FILEPATH}' output correctly. Line: {cpuUsageLine}");
                    return;
                }

                var cpuUsageQuotaUs = long.Parse(cpuUsageQuota);

                if (string.IsNullOrWhiteSpace(cpuUsagePeriod))
                {
                    _logger.LogWarning($"Couldn't read line from '{CGROUP_V1_CFS_PERIOD_FILEPATH}'");
                    return;
                }

                if (long.TryParse(cpuUsagePeriod, out _))
                {
                    _logger.LogWarning($"Failed to parse '{CGROUP_V1_CFS_PERIOD_FILEPATH}' output correctly. Line: {cpuUsageLine}");
                    return;
                }

                var cpuUsagePeriodUs = long.Parse(cpuUsagePeriod);

                if (i > 0)
                {
                    var deltaTotalTime = cpuUsageNs - _prevTotalTime;

                    // 1 ns = 1000us
                    // 1 s = 1000000us
                    // Total cpu time quota: cfs_quota_us * (MONITOR_PERIOD / cfs_period_us)
                    var currentCpuUsage = ((deltaTotalTime * 1_000) / (cpuUsageQuotaUs * ((float)MONITOR_PERIOD.Seconds * 1_000_000) / cpuUsagePeriodUs)) * 100f;

                    var previousCpuUsage = CpuUsage ?? 0f;
                    CpuUsage = (previousCpuUsage + 2 * currentCpuUsage) / 3;
                }

                _prevTotalTime = cpuUsageNs;
            }
            else
            {
                var cpuUsageLine = await ReadLineStartingWithAsync(CPUSTAT_FILEPATH, "cpu  ");

                if (string.IsNullOrWhiteSpace(cpuUsageLine))
                {
                    _logger.LogWarning($"Couldn't read line from '{CPUSTAT_FILEPATH}'");
                    return;
                }

                // Format: "cpu  20546715 4367 11631326 215282964 96602 0 584080 0 0 0"
                var cpuNumberStrings = cpuUsageLine.Split(' ').Skip(2);

                if (cpuNumberStrings.Any(n => !long.TryParse(n, out _)))
                {
                    _logger.LogWarning($"Failed to parse '{CPUSTAT_FILEPATH}' output correctly. Line: {cpuUsageLine}");
                    return;
                }

                var cpuNumbers = cpuNumberStrings.Select(long.Parse).ToArray();
                var idleTime = cpuNumbers[3];
                var iowait = cpuNumbers[4]; // Iowait is not real cpu time
                var totalTime = cpuNumbers.Sum() - iowait;

                if (i > 0)
                {
                    var deltaIdleTime = idleTime - _prevIdleTime;
                    var deltaTotalTime = totalTime - _prevTotalTime;

                    var currentCpuUsage = (1.0f - deltaIdleTime / ((float)deltaTotalTime)) * 100f;

                    var previousCpuUsage = CpuUsage ?? 0f;
                    CpuUsage = (previousCpuUsage + 2 * currentCpuUsage) / 3;
                }

                _prevIdleTime = idleTime;
                _prevTotalTime = totalTime;

            }
        }

        private async Task UpdateAvailableMemory()
        {
            if (InCGroup)
            {
                var memTotalLine = await ReadLineAsync(CGROUP_V1_USED_MEM_FILEPATH);

                if (string.IsNullOrWhiteSpace(memTotalLine))
                {
                    _logger.LogWarning($"Couldn't read line from '{CGROUP_V1_USED_MEM_FILEPATH}'");
                    return;
                }

                if (!long.TryParse(new string(memTotalLine.Where(char.IsDigit).ToArray()), out var usedMemInB))
                {
                    _logger.LogWarning($"Couldn't parse memory.usage_in_bytes output");
                    return;
                }

                AvailableMemory = usedMemInB;
            }
            else
            {
                var memAvailableLine = await ReadLineStartingWithAsync(MEMINFO_FILEPATH, "MemAvailable");

                if (string.IsNullOrWhiteSpace(memAvailableLine))
                {
                    memAvailableLine = await ReadLineStartingWithAsync(MEMINFO_FILEPATH, "MemFree");
                    if (string.IsNullOrWhiteSpace(memAvailableLine))
                    {
                        _logger.LogWarning($"Couldn't read 'MemAvailable' or 'MemFree' line from '{MEMINFO_FILEPATH}'");
                        return;
                    }
                }

                if (!long.TryParse(new string(memAvailableLine.Where(char.IsDigit).ToArray()), out var availableMemInKb))
                {
                    _logger.LogWarning($"Couldn't parse meminfo output: '{memAvailableLine}'");
                    return;
                }

                AvailableMemory = availableMemInKb * 1_000;
            }
        }

        private void WriteToStatistics()
        {
            FloatValueStatistic.FindOrCreate(StatisticNames.RUNTIME_CPUUSAGE, () => CpuUsage.Value);
            IntValueStatistic.FindOrCreate(StatisticNames.RUNTIME_GC_TOTALMEMORYKB, () => (long)((MemoryUsage + KB - 1.0) / KB)); // Round up

#if LOG_MEMORY_PERF_COUNTERS
            IntValueStatistic.FindOrCreate(StatisticNames.RUNTIME_MEMORY_TOTALPHYSICALMEMORYMB, () => (long)((TotalPhysicalMemory / KB) / KB));
            IntValueStatistic.FindOrCreate(StatisticNames.RUNTIME_MEMORY_AVAILABLEMEMORYMB, () => (long)((AvailableMemory / KB) / KB)); // Round up
#endif

            IntValueStatistic.FindOrCreate(StatisticNames.RUNTIME_DOT_NET_THREADPOOL_INUSE_WORKERTHREADS, () =>
            {
                ThreadPool.GetMaxThreads(out var maXworkerThreads, out var maXcompletionPortThreads);

                // GetAvailableThreads Retrieves the difference between the maximum number of thread pool threads
                // and the number currently active.
                // So max-Available is the actual number in use. If it goes beyond min, it means we are stressing the thread pool.
                ThreadPool.GetAvailableThreads(out var workerThreads, out var completionPortThreads);
                return maXworkerThreads - workerThreads;
            });
            IntValueStatistic.FindOrCreate(StatisticNames.RUNTIME_DOT_NET_THREADPOOL_INUSE_COMPLETIONPORTTHREADS, () =>
            {
                ThreadPool.GetMaxThreads(out var maxWorkerThreads, out var maxCompletionPortThreads);

                ThreadPool.GetAvailableThreads(out var workerThreads, out var completionPortThreads);
                return maxCompletionPortThreads - completionPortThreads;
            });
        }

        private async Task Monitor(CancellationToken ct)
        {
            int i = 0;
            while (true)
            {
                if (ct.IsCancellationRequested)
                    throw new TaskCanceledException("Monitor task canceled");

                try
                {
                    await Task.WhenAll(
                        i == 0 ? UpdateTotalPhysicalMemory() : Task.CompletedTask,
                        UpdateCpuUsage(i),
                        UpdateAvailableMemory()
                    );

                    if (i == 1)
                        WriteToStatistics();

                    var logStr = $"LinuxEnvironmentStatistics: CpuUsage={CpuUsage?.ToString("0.0")}, TotalPhysicalMemory={TotalPhysicalMemory}, AvailableMemory={AvailableMemory}";
                    _logger.LogTrace(logStr);

                    await Task.Delay(MONITOR_PERIOD, ct);
                }
                catch (Exception ex) when (ex.GetType() != typeof(TaskCanceledException))
                {
                    _logger.LogError(ex, "LinuxEnvironmentStatistics: error");
                    await Task.Delay(MONITOR_PERIOD + MONITOR_PERIOD + MONITOR_PERIOD, ct);
                }
                if (i < 2)
                    i++;
            }
        }

        private static async Task<string> ReadLineAsync(string path)
        {
            using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 512, FileOptions.SequentialScan | FileOptions.Asynchronous))
            using (var r = new StreamReader(fs, Encoding.ASCII))
            {
                var line = await r.ReadLineAsync();
                if (line is not null)
                {
                    return await r.ReadLineAsync();
                }
            }

            return null;
        }

        private static async Task<string> ReadLineStartingWithAsync(string path, string lineStartsWith)
        {
            using (var fs = new FileStream(path, FileMode.Open, FileAccess.Read, FileShare.Read, 512, FileOptions.SequentialScan | FileOptions.Asynchronous))
            using (var r = new StreamReader(fs, Encoding.ASCII))
            {
                string line;
                while ((line = await r.ReadLineAsync()) != null)
                {
                    if (line.StartsWith(lineStartsWith))
                        return line;
                }
            }

            return null;
        }
    }
}
