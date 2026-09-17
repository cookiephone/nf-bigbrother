package nextflow.bigbrother

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.time.Duration
import java.time.Instant
import java.time.ZonedDateTime
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.processor.TaskHandler
import nextflow.processor.TaskRun
import nextflow.script.params.FileOutParam
import nextflow.trace.TraceObserver
import nextflow.trace.TraceRecord

/**
 * Builds the physical execution graph of a run: one node per task, an edge
 * wherever one task's output file is another task's input. After every task it
 * writes a JSON snapshot and a matching DOT of the graph so far, so the run can
 * be watched as it goes. Also records each task's resource usage and machine.
 */
@Slf4j
@CompileStatic
class BigBrotherObserver implements TraceObserver {

    private final WfInstance wf = new WfInstance()

    // The callbacks run on many task threads at once, so everything that touches
    // the model or the snapshot counter goes through this lock.
    private final Object lock = new Object()

    private Session session
    private BigBrotherConfig config
    private Path outputDir
    private String safeName
    private ZonedDateTime startTime
    private int snapshotCounter = 0

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

        log.info "[BigBrother] monitoring run '${projectName}' -> ${outputDir}"
    }

    @Override
    void onProcessSubmit(TaskHandler handler, TraceRecord trace) {
        final TaskRun task = handler.task
        synchronized (lock) {
            final TaskSpecification spec = wf.touchTaskSpecification(task.id.toString())
            spec.name = task.name
            spec.inputFiles = collectInputFiles(task)
            registerFiles(spec.inputFiles)
        }
    }

    @Override
    void onProcessComplete(TaskHandler handler, TraceRecord trace) {
        synchronized (lock) {
            recordTask(handler.task, trace)
            writeSnapshot()
        }
    }

    @Override
    void onProcessCached(TaskHandler handler, TraceRecord trace) {
        synchronized (lock) {
            recordTask(handler.task, trace)
            writeSnapshot()
        }
    }

    @Override
    void onFlowComplete() {
        synchronized (lock) {
            wf.makespanInSeconds = Duration.between(startTime, ZonedDateTime.now(ZoneOffset.UTC)).seconds.toString()
            writeInstance('complete')
            log.info "[BigBrother] run complete: ${wf.taskSpecifications.size()} tasks recorded in ${outputDir}"
        }
    }

    @Override
    void onFlowError(TaskHandler handler, TraceRecord trace) {
        synchronized (lock) {
            if (handler != null) {
                recordTask(handler.task, trace)
            }
            writeInstance('error')
            log.warn '[BigBrother] run failed; error snapshot written'
        }
    }

    @Override
    boolean enableMetrics() { return true }

    // --- helpers ---

    // Record a task's files, resource metrics and machine.
    private void recordTask(TaskRun task, TraceRecord trace) {
        final String id = task.id.toString()

        final TaskSpecification spec = wf.touchTaskSpecification(id)
        if (!spec.name) {
            spec.name = task.name
        }
        spec.inputFiles = collectInputFiles(task)
        spec.outputFiles = collectOutputFiles(task)
        registerFiles(spec.inputFiles)
        registerFiles(spec.outputFiles)

        final TaskExecution exec = wf.touchTaskExecution(id)
        exec.runtimeInSeconds = (traceLong(trace, 'realtime') / 1000.0d) as float
        final Long start = trace.get('start') as Long
        if (start != null) {
            exec.executedAt = Instant.ofEpochMilli(start).atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT)
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

        final String node = recordMachine(task.workDir)
        if (node) {
            exec.machines = [node] as String[]
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

    private void registerFiles(List<String> files) {
        files.each { file ->
            long size = -1L
            try {
                size = Files.size(Paths.get(file))
            }
            catch (Exception ignored) {
            }
            wf.touchFileSpecification(file, size)
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

    private static long traceLong(TraceRecord trace, String key) {
        final Object value = trace.get(key)
        if (value == null) {
            return 0L
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
        try {
            return (value as String).toFloat()
        }
        catch (NumberFormatException ignored) {
            return 0f
        }
    }

    // --- writing snapshots ---

    private void writeSnapshot() {
        if (!config.emitPartials) {
            return
        }
        writeInstance("partial_${snapshotCounter.toString().padLeft(3, '0')}")
        snapshotCounter++
    }

    private void writeInstance(String phase) {
        final String base = config.prefix ? "${config.prefix}_${phase}_${safeName}_${session.uniqueId}"
            : "${phase}_${safeName}_${session.uniqueId}"
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
