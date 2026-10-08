package nextflow.bigbrother

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardOpenOption
import java.time.Duration
import java.time.Instant
import java.time.ZonedDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

import groovy.json.JsonOutput
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.processor.TaskRun
import nextflow.script.params.FileOutParam
import nextflow.trace.TraceObserverV2
import nextflow.trace.TraceRecord
import nextflow.trace.event.TaskEvent

/**
 * Builds the physical execution graph of a run: one node per task, an edge
 * wherever one task's output file is another task's input. After every task it
 * writes a JSON snapshot and a matching DOT of the graph so far, so the run can
 * be watched as it goes. Also records each task's resource usage and machine.
 */
@Slf4j
@CompileStatic
class BigBrotherObserver implements TraceObserverV2 {

    private final WfInstance wf = new WfInstance()

    // The callbacks run on many task threads at once, so everything that touches
    // the model or the snapshot counter goes through this lock.
    private final Object lock = new Object()

    // When each task entered Nextflow's own queue. The trace record's `submit` is
    // when the executor handed it to the backend, so the two together separate
    // the wait inside Nextflow from the wait inside the scheduler.
    private final Map<String, String> pendingAt = [:]

    private Session session
    private BigBrotherConfig config
    private Path outputDir
    private String safeName
    private ZonedDateTime startTime
    private int snapshotCounter = 0
    private int completedCount = 0
    private Path eventLog

    @Override
    void onFlowCreate(Session session) {
        this.session = session
        this.config = new BigBrotherConfig(session.config.get('bigbrother') as Map)
        this.outputDir = Paths.get(config.outputDir).toAbsolutePath()
        Files.createDirectories(outputDir)

        this.startTime = ZonedDateTime.now(ZoneOffset.UTC)
        final String now = startTime.format(DateTimeFormatter.ISO_INSTANT)
        final String projectName = session.workflowMetadata?.projectName ?:
            session.workflowMetadata?.scriptName ?: 'workflow'
        this.safeName = projectName.replaceAll(/[\/\\?%*:|"<>\s]/, '_')

        wf.name = projectName
        wf.description = 'BigBrother physical execution trace'
        wf.createdAt = now
        wf.executedAt = now
        wf.schemaVersion = '1.5'
        wf.runtimeSystemName = 'Nextflow'
        wf.runtimeSystemUrl = 'https://www.nextflow.io/'
        wf.runtimeSystemVersion = session.workflowMetadata?.nextflow?.version?.toString() ?: ''

        if (config.emitEvents) {
            this.eventLog = outputDir.resolve("${baseName('events')}.jsonl")
            appendEvent(wf.runEventMap())
        }

        log.info "[BigBrother] monitoring run '${projectName}' -> ${outputDir}"
    }

    @Override
    void onTaskPending(TaskEvent event) {
        final TaskRun task = event?.handler?.task
        if (task == null) {
            return
        }
        final String now = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT)
        synchronized (lock) {
            pendingAt[task.id.toString()] = now
        }
    }

    @Override
    void onTaskSubmit(TaskEvent event) {
        final TaskRun task = event?.handler?.task
        if (task == null) {
            return
        }
        // Walking the filesystem and stat-ing files happens before the lock is
        // taken: it is the slow part, it needs nothing from the model, and
        // holding the lock across it would serialise every concurrent task.
        final String id = task.id.toString()
        final List<String> inputs = collectInputFiles(task)
        final Map<String, Long> sizes = statFiles(inputs)
        synchronized (lock) {
            final TaskSpecification spec = wf.touchTaskSpecification(id)
            spec.name = task.name
            spec.inputFiles = inputs
            registerFiles(inputs, sizes)
            wf.linkTask(id, inputs, null)
        }
    }

    @Override
    void onTaskComplete(TaskEvent event) {
        final TaskRun task = event?.handler?.task
        if (task != null) {
            recordAndEmit(task, event.trace)
        }
    }

    @Override
    void onTaskCached(TaskEvent event) {
        final TaskRun task = event?.handler?.task
        if (task != null) {
            recordAndEmit(task, event.trace)
        }
    }

    // Collect a finished task's files off the lock, then take the lock just long
    // enough to fold it into the model and write it out.
    private void recordAndEmit(TaskRun task, TraceRecord trace) {
        final List<String> inputs = collectInputFiles(task)
        final List<String> outputs = collectOutputFiles(task)
        final Map<String, Long> sizes = statFiles(inputs, outputs)
        synchronized (lock) {
            recordTask(task, trace, inputs, outputs, sizes)
            completedCount++
            if (config.emitEvents) {
                appendEvent(wf.taskEventMap(task.id.toString()))
            }
            if (config.emitPartials && completedCount % config.snapshotEvery == 0) {
                writeSnapshot()
            }
        }
    }

    @Override
    void onFlowComplete() {
        synchronized (lock) {
            wf.makespanInSeconds = Duration.between(startTime, ZonedDateTime.now(ZoneOffset.UTC)).seconds.toString()
            writeInstance('complete')
            if (config.emitEvents) {
                appendEvent(wf.endEventMap('complete'))
            }
            log.info "[BigBrother] run complete: ${wf.taskSpecifications.size()} tasks recorded in ${outputDir}"
        }
    }

    @Override
    void onFlowError(TaskEvent event) {
        final TaskRun task = event?.handler?.task
        final List<String> inputs = task != null ? collectInputFiles(task) : null
        final List<String> outputs = task != null ? collectOutputFiles(task) : null
        final Map<String, Long> sizes = task != null ? statFiles(inputs, outputs) : null
        synchronized (lock) {
            if (task != null) {
                recordTask(task, event.trace, inputs, outputs, sizes)
                if (config.emitEvents) {
                    appendEvent(wf.taskEventMap(task.id.toString()))
                }
            }
            writeInstance('error')
            if (config.emitEvents) {
                appendEvent(wf.endEventMap('error'))
            }
            log.warn '[BigBrother] run failed; error snapshot written'
        }
    }

    @Override
    boolean enableMetrics() { return true }

    // --- helpers ---

    // Fold a task's files, resource metrics and machine into the model. The
    // caller has already done the filesystem work, off the lock.
    private void recordTask(TaskRun task, TraceRecord trace, List<String> inputs,
                            List<String> outputs, Map<String, Long> sizes) {
        final String id = task.id.toString()

        final TaskSpecification spec = wf.touchTaskSpecification(id)
        if (!spec.name) {
            spec.name = task.name
        }
        spec.inputFiles = inputs
        spec.outputFiles = outputs
        registerFiles(inputs, sizes)
        registerFiles(outputs, sizes)
        wf.linkTask(id, inputs, outputs)

        final TaskExecution exec = wf.touchTaskExecution(id)
        exec.pendingAt = pendingAt[id] ?: ''
        exec.executor = task.processor?.executor?.name ?: ''
        exec.processName = task.processor?.name ?: ''

        final String node = recordMachine(task.workDir)
        if (node) {
            exec.machines = [node] as String[]
        }

        // A failing run can reach onFlowError without a trace record.
        if (trace == null) {
            return
        }

        exec.runtimeInSeconds = (traceLong(trace, 'realtime') / 1000.0d) as float
        final Long start = trace.get('start') as Long
        if (start != null) {
            exec.executedAt = isoMillis(start)
        }
        exec.avgCPU = traceFloat(trace, '%cpu')
        exec.readBytes = traceLong(trace, 'rchar')
        exec.writtenBytes = traceLong(trace, 'wchar')
        exec.memoryInBytes = traceLong(trace, 'rss')
        exec.peakVmem = traceLong(trace, 'peak_vmem')
        exec.peakRss = traceLong(trace, 'peak_rss')
        exec.volCtxt = traceLong(trace, 'vol_ctxt')
        exec.invCtxt = traceLong(trace, 'inv_ctxt')
        exec.commandProgram = trace.get('script')?.toString() ?: ''
        exec.commandArguments = [] as String[]

        // What the task asked for. Nextflow fills these from the process
        // directives on every executor, so they need no wrapper patch.
        exec.requestedCpus = traceLong(trace, 'cpus') as int
        exec.requestedMemoryBytes = traceLong(trace, 'memory')
        exec.requestedDiskBytes = traceLong(trace, 'disk')
        exec.requestedTimeMillis = traceLong(trace, 'time')

        final Long submit = trace.get('submit') as Long
        final Long complete = trace.get('complete') as Long
        if (submit != null) {
            exec.submittedAt = isoMillis(submit)
        }
        if (complete != null) {
            exec.completedAt = isoMillis(complete)
        }
        if (submit != null && start != null) {
            exec.queueWaitSeconds = ((start - submit) / 1000.0d) as float
        }
        exec.durationSeconds = (traceLong(trace, 'duration') / 1000.0d) as float

        exec.attempt = traceLong(trace, 'attempt') as int
        exec.exitStatus = traceString(trace, 'exit')
        exec.status = traceString(trace, 'status')
        exec.errorAction = traceString(trace, 'error_action')

        exec.queue = traceString(trace, 'queue')
        exec.container = traceString(trace, 'container')
        exec.cpuModel = traceString(trace, 'cpu_model')
        // Declared by Nextflow but populated by no built-in executor; kept so it
        // fills in by itself if one ever starts setting it. The machine details
        // from the wrapper patch are what actually identify the node today.
        exec.hostname = traceString(trace, 'hostname')
        exec.nativeId = traceString(trace, 'native_id')

        exec.tag = traceString(trace, 'tag')
        exec.taskHash = traceString(trace, 'hash')
        if (!exec.processName) {
            exec.processName = traceString(trace, 'process')
        }
    }

    // A task's staged inputs, as resolved source paths with directories expanded.
    private List<String> collectInputFiles(TaskRun task) {
        List<Path> paths = new ArrayList<>(task.getInputFilesMap().values())
        return expandToFiles(paths)
    }

    // Output files come from the collected FileOutParam values (real paths, globs
    // already expanded). getOutputFilesNames() only returns the glob patterns,
    // which never match anything on disk.
    private List<String> collectOutputFiles(TaskRun task) {
        List<Path> paths = []
        task.getOutputsByType(FileOutParam).values().each { value -> flattenPaths(value, paths) }
        return expandToFiles(paths)
    }

    private static void flattenPaths(Object value, List<Path> acc) {
        if (value == null) {
            return
        }
        if (value instanceof Path) {
            acc << (Path) value
        }
        else if (value instanceof File) {
            acc << ((File) value).toPath()
        }
        else if (value instanceof CharSequence) {
            // a plain string output (e.g. stdout) is not a file
        }
        else if (value instanceof Map) {
            ((Map) value).values().each { v -> flattenPaths(v, acc) }
        }
        else if (value instanceof Iterable) {
            ((Iterable) value).each { v -> flattenPaths(v, acc) }
        }
        else if (value instanceof Object[]) {
            ((Object[]) value).each { v -> flattenPaths(v, acc) }
        }
    }

    // Replace directories with the files inside them; leave plain files alone.
    private List<String> expandToFiles(List<Path> paths) {
        List<String> result = []
        paths.each { path ->
            try {
                if (Files.isDirectory(path)) {
                    Files.walk(path).withCloseable { stream ->
                        stream.filter { Path p -> Files.isRegularFile(p) }
                            .forEach { Path p -> result << p.toString() }
                    }
                }
                else if (Files.isRegularFile(path)) {
                    result << path.toString()
                }
            }
            catch (Exception e) {
                log.trace "[BigBrother] could not expand ${path}: ${e.message}"
            }
        }
        return result.unique()
    }

    // Stat a run's worth of files. Called before the lock is taken, since this
    // touches the filesystem once per file and the model is not involved.
    private static Map<String, Long> statFiles(List<String> first, List<String> second = null) {
        final Map<String, Long> sizes = new HashMap<>()
        [first, second].each { group ->
            group?.each { file ->
                if (file == null || sizes.containsKey(file)) {
                    return
                }
                long size = -1L
                try {
                    size = Files.size(Paths.get(file))
                }
                catch (Exception ignored) {
                }
                sizes.put(file, size)
            }
        }
        return sizes
    }

    private void registerFiles(List<String> files, Map<String, Long> sizes) {
        files?.each { file ->
            wf.touchFileSpecification(file, sizes.containsKey(file) ? sizes.get(file) : -1L)
        }
    }

    // Read the bblog.log that the patched command wrapper leaves in the work dir.
    private String recordMachine(Path workDir) {
        final Path logFile = workDir?.resolve('bblog.log')
        if (logFile == null || !Files.exists(logFile)) {
            return null
        }
        Map<String, String> kv = [:]
        try {
            Files.readAllLines(logFile).each { line ->
                final int idx = line.indexOf(':')
                if (idx > 0) {
                    kv[line.substring(0, idx).trim()] = line.substring(idx + 1).trim()
                }
            }
        }
        catch (Exception e) {
            log.trace "[BigBrother] could not read ${logFile}: ${e.message}"
            return null
        }

        final String node = kv['nodeName']
        if (!node) {
            return null
        }
        final MachineSpecification machine = wf.touchMachineSpecification(node)
        machine.system = kv['system'] ?: machine.system
        machine.architecture = kv['architecture'] ?: machine.architecture
        machine.release = kv['release'] ?: machine.release
        machine.cpuVendor = kv['cpuVendor'] ?: machine.cpuVendor
        machine.bootID = kv['boot-id'] ?: machine.bootID
        if (kv['memoryInBytes']) {
            machine.memoryInBytes = kv['memoryInBytes'].toLong()
        }
        if (kv['cpuCoreCount']) {
            machine.cpuCoreCount = kv['cpuCoreCount'].toFloat()
        }
        if (kv['cpuSpeedInMHz']) {
            machine.cpuSpeedInMHz = kv['cpuSpeedInMHz'].toFloat()
        }
        return node
    }

    // TraceRecord stores typed values: 'num'/'mem'/'time' fields arrive as
    // Numbers, but a few are strings. Going via toString() would turn a Double
    // like 6.0 into an unparseable "6.0", so Numbers are read directly.
    private static long traceLong(TraceRecord trace, String key) {
        final Object value = trace.get(key)
        if (value == null) {
            return 0L
        }
        if (value instanceof Number) {
            return ((Number) value).longValue()
        }
        try {
            return (value as String).toLong()
        }
        catch (NumberFormatException ignored) {
            return 0L
        }
    }

    private static float traceFloat(TraceRecord trace, String key) {
        final Object value = trace.get(key)
        if (value == null) {
            return 0f
        }
        if (value instanceof Number) {
            return ((Number) value).floatValue()
        }
        try {
            return (value as String).toFloat()
        }
        catch (NumberFormatException ignored) {
            return 0f
        }
    }

    private static String traceString(TraceRecord trace, String key) {
        final Object value = trace.get(key)
        return value != null ? value.toString() : ''
    }

    private static String isoMillis(long epochMillis) {
        return Instant.ofEpochMilli(epochMillis).atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT)
    }

    // --- writing output ---

    // Append one JSON object as a single line. Done under the same lock as the
    // model update so lines land in the order tasks finished.
    private void appendEvent(Map event) {
        if (eventLog == null) {
            return
        }
        try {
            final String line = JsonOutput.toJson(event) + '\n'
            Files.write(eventLog, line.getBytes('UTF-8'), StandardOpenOption.CREATE, StandardOpenOption.APPEND)
        }
        catch (Exception e) {
            log.warn "[BigBrother] failed to append to ${eventLog}: ${e.message}"
        }
    }

    private String baseName(String phase) {
        return config.prefix ? "${config.prefix}_${phase}_${safeName}_${session.uniqueId}"
            : "${phase}_${safeName}_${session.uniqueId}"
    }

    private void writeSnapshot() {
        writeInstance("partial_${snapshotCounter.toString().padLeft(3, '0')}")
        snapshotCounter++
    }

    private void writeInstance(String phase) {
        final String base = baseName(phase)
        try {
            Files.write(outputDir.resolve("${base}.json"), wf.toJson().getBytes('UTF-8'))
            if (config.emitDot) {
                Files.write(outputDir.resolve("${base}.dot"), wf.renderPhysicalDot().getBytes('UTF-8'))
            }
        }
        catch (Exception e) {
            log.warn "[BigBrother] failed to write snapshot ${base}: ${e.message}"
        }
    }

}
