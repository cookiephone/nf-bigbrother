/* groovylint-disable ClassJavadoc, UnnecessaryObjectReferences */
package nextflow.bigbrother

import static nextflow.bigbrother.ReflectUtils.bypassProtected

import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.LinkOption
import java.time.ZonedDateTime
import java.time.ZoneOffset
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.time.Duration

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.cache.CacheDB
import nextflow.trace.TraceObserver
import nextflow.trace.TraceRecord
import nextflow.script.ProcessConfig
import nextflow.script.ScriptMeta
import nextflow.processor.TaskHandler
import nextflow.processor.TaskProcessor
import nextflow.container.resolver.ContainerInfo
import nextflow.dag.DotRenderer
import nextflow.dag.DAG

@Slf4j
@CompileStatic
class BigBrotherObserver implements TraceObserver {

    private final WfInstance wf = new WfInstance()

    private Session session
    private String logstr = ''
    private final Map<String, List<String>> taskInputs = [:]
    private final Map<String, List<String>> taskOutputs = [:]
    private final Map<String, Long> fileSizes = [:]
    private ZonedDateTime startTime

    void logCache() {
        CacheDB cache = this.session.cache
        cache.eachRecord { key, trace, refcount ->
            logInfo "[cache]: $key $trace $refcount"
        }
    }

    void logSession() {
        logInfo " - session UUID: ${this.session.uniqueId}"
        logInfo " - session output dir: ${this.session.outputDir}"
        logInfo " - session work dir: ${this.session.workDir}"
        logInfo " - session bucket dir: ${this.session.bucketDir}"
        logInfo " - session base dir: ${this.session.baseDir}"
        logInfo " - session lib dir: ${this.session.libDir}"
        logInfo " - session classes dir: ${this.session.classesDir}"
        logInfo " - session bin dir: ${this.session.binDir}"
        logInfo " - session bin entries: ${this.session.binEntries}"
        logInfo " - session cloud path for cached metadata: ${this.session.cloudCachePath}"
        logInfo " - session disable uploading bin when using cloud executor: ${this.session.disableRemoteBinDir}"
        logInfo " - session script name: ${this.session.scriptName}"
        logInfo " - session pool size: ${this.session.poolSize}"
        logInfo " - session config env: ${this.session.configEnv}"
        logInfo " - session mainfest: ${this.session.manifest.toMap()}"
        logInfo " - session abstract DAG: ${new DotRenderer(this.session.uniqueId.toString()).renderNetwork(this.session.dag)}"
        ['docker', 'podman', 'sarus', 'shifter', 'udocker', 'singularity', 'apptainer', 'charliecloud'].each { engine ->
            logInfo " - session container config ($engine): ${this.session.getContainerConfig(engine)}"
        }
        logInfo " - session containers: ${this.session.fetchContainers()}"
    }

    void logTaskProcessor(TaskProcessor process) {
        logInfo " - process (${process.id}): name: ${process.name}"
        logInfo " - process (${process.id}): singleton ${process.singleton}"
        logInfo " - process (${process.id}): forks count ${process.forksCount} (max: ${process.maxForks})"
        logInfo " - process (${process.id}): bin dirs: ${bypassProtected(process, 'getBinDirs')}"
        logInfo " - process (${process.id}): is local workdir: ${bypassProtected(process, 'isLocalWorkDir')}"
        logInfo " - process (${process.id}): environment: ${process.processEnvironment}"
        logInfo " - process (${process.id}): cacheable: ${process.cacheable}"
        logInfo " - process (${process.id}): resumable: ${bypassProtected(process, 'isResumable')}"
        logInfo " - process (${process.id}): inputs: ${process.config.getInputs().names}"
        logInfo " - process (${process.id}): outputs: ${process.config.getOutputs().names}"
        ProcessConfig.DIRECTIVES.each { directive ->
            String value = process?.config?.getProperty(directive as String).toString()
            logInfo " - process (${process.id}): $directive: ${value}"
        }
        logInfo " - process (${process.id}): owner script: ${process.ownerScript}"
        ScriptMeta meta = ScriptMeta.get(process.ownerScript)
        logInfo " - process (${process.id}): owner script path: ${meta.scriptPath}"
        logInfo " - process (${process.id}): owner script name: ${process.ownerScript.getClass().getName()}"
        logInfo " - process (${process.id}): owner script module path: ${meta.scriptPath?.parent}"
        logInfo " - process (${process.id}): owner script definitions: ${meta.definitions}"
        logInfo " - process (${process.id}): owner script imports: ${bypassProtected(meta, 'imports')}"
        logInfo " - process (${process.id}): owner script is module: ${meta.module}"
        logInfo " - process (${process.id}): executor name: ${process.executor.name}"
        logInfo " - process (${process.id}): executor stage dir: ${process.executor.getStageDir()}"
        logInfo " - process (${process.id}): containerization managed by the executor: ${process.executor.isContainerNative()}"
        logInfo " - process (${process.id}): container engine setting: ${process.executor.containerConfigEngine()}"
        logInfo " - process (${process.id}): fusion filesystem enabled: ${process.executor.isFusionEnabled()}"
        logInfo " - process (${process.id}): code: ${process.taskBody.source}"
        logInfo " - process (${process.id}): script type: ${process.taskBody.type}"
        logInfo " - process (${process.id}): is shell: ${process.taskBody.isShell}"
    }

    void logTaskHandler(TaskHandler handler) {
        logInfo " - handler (task ID ${handler.task.id}): is array child: ${handler.isArrayChild}"
        logInfo " - handler (task ID ${handler.task.id}): submit time (ms): ${handler.submitTimeMillis}"
        logInfo " - handler (task ID ${handler.task.id}): start time (ms): ${handler.startTimeMillis}"
        logInfo " - handler (task ID ${handler.task.id}): complete time (ms): ${handler.completeTimeMillis}"
        logInfo " - handler (task ID ${handler.task.id}): status: ${handler.status}"
        logInfo " - handler (task ID ${handler.task.id}): task name: ${handler.task.name}"
        logInfo " - handler (task ID ${handler.task.id}): task aborted: ${handler.task.aborted}"
        logInfo " - handler (task ID ${handler.task.id}): task failed: ${handler.task.failed}"
        logInfo " - handler (task ID ${handler.task.id}): task status: ${handler.task.exitStatus}"
        logInfo " - handler (task ID ${handler.task.id}): task work dir: ${handler.task.workDir}"
        Map<String, Path> inputFilesMap = handler.task.getInputFilesMap()
        Map<String, Long> inputFileSizes = [:]
        inputFilesMap.each { name, path ->
            try {
                Path realPath = path.toRealPath(LinkOption.NOFOLLOW_LINKS)
                long size = Files.size(realPath)
                inputFileSizes[name] = size
            }
            catch (IOException e) {
                inputFileSizes[name] = -1L
            }
        }
        List<String> outputFileNames = handler.task.getOutputFilesNames()
        logInfo " - handler (task ID ${handler.task.id}): task input files: $inputFilesMap"
        logInfo " - handler (task ID ${handler.task.id}): task input file sizes: $inputFileSizes"
        logInfo " - handler (task ID ${handler.task.id}): task output files: $outputFileNames"
        List<Path> inputPaths = inputFilesMap.values() as List
        List<Path> outputPaths = outputFileNames.collect { str -> handler.task.workDir.resolve(str) }
        taskInputs[handler.task.id.toString()] = expandPathsToFiles(inputPaths)*.toString()
        taskOutputs[handler.task.id.toString()] = expandPathsToFiles(outputPaths)*.toString()
        expandPathsToFiles(inputPaths).each { path ->
            try {
                Path realPath = path.toRealPath(LinkOption.NOFOLLOW_LINKS)
                long size = Files.size(realPath)
                fileSizes[realPath.toString()] = size
            } catch (IOException e) {
                fileSizes[path.toString()] = -1L
            }
        }
        logInfo " - handler (task ID ${handler.task.id}): task container key: ${handler.task.containerKey}"
        ContainerInfo info = bypassProtected(handler.task, 'containerInfo') as ContainerInfo
        logInfo " - handler (task ID ${handler.task.id}): task container info - source: ${info.source}"
        logInfo " - handler (task ID ${handler.task.id}): task container info - target: ${info.target}"
        logInfo " - handler (task ID ${handler.task.id}): task container info - hash key: ${info.hashKey}"
        String taskIdStr = handler.task.id
        TaskSpecification task = this.wf.touchTaskSpecification(taskIdStr)
        task.inputFiles = taskInputs[taskIdStr]
        task.outputFiles = taskOutputs[taskIdStr]
        task.inputFiles.each { file ->
            def size = fileSizes[file] != null ? fileSizes[file] : -1L
            this.wf.touchFileSpecification(file, size)
        }
        task.outputFiles.each { file ->
            def size = fileSizes[file] != null ? fileSizes[file] : -1L
            this.wf.touchFileSpecification(file, size)
        }
    }

    void logTraceRecord(TraceRecord trace) {
        String raw = trace
        String content = raw.replaceAll(/^.*?\[|\]$/, '')
        String[] parts = content.split(/,\s*(?=\w+:)/)
        logInfo ' - trace record:'
        parts.each { entry ->
            logInfo " - $entry"
        }
    }

    @Override
    void onFlowCreate(Session session) {
        logInfo 'pipeline created'
        this.session = session
        logSession()
        logCache()
        this.startTime = ZonedDateTime.now(ZoneOffset.UTC)
        String time = ZonedDateTime.now(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT)
        this.wf.name = session.workflowMetadata.projectName
        this.wf.description = 'BigBrother nextflow trace'
        this.wf.createdAt = time
        this.wf.schemaVersion = '1.5'
        this.wf.runtimeSystemName = 'Nextflow'
        this.wf.runtimeSystemUrl = 'https://www.nextflow.io/'
        this.wf.runtimeSystemVersion = "${session.getManifest().getNextflowVersion()?.trim()}"
        this.wf.executedAt = time
    }

    @Override
    void onFlowBegin() {
        logInfo 'pipeline started'
    }

    @Override
    void onFlowComplete() {
        logInfo 'pipeline complete'
        this.wf.makespanInSeconds = "${Duration.between(this.startTime, ZonedDateTime.now(ZoneOffset.UTC)).seconds}"
        //String dotstring = new DotRenderer(this.session.uniqueId.toString()).renderNetwork(this.session.dag)
        //logInfo " - abstract DAG: $dotstring"
        log.info logstr
        log.info "taskid-to-inputs: $taskInputs"
        log.info "taskid-to-outputs: $taskOutputs"
        log.info "path-to-filesize: $fileSizes"
        log.info '-------- [instance json] --------'
        log.info this.wf.toJson()

        def rawProjectName = session.workflowMetadata.projectName
        def safeProjectName = rawProjectName.replaceAll(/[\/\\?%*:|"<>]/, '_')
        def filename = "${safeProjectName}_${session.uniqueId}.json"
        new File(filename).text = this.wf.toJson()
    }

    @Override
    void onProcessCreate(TaskProcessor process) {
        logInfo "process created (Name: ${process.name}, ID: ${process.id})"
        logTaskProcessor(process)
    }

    @Override
    void onProcessTerminate(TaskProcessor process) {
        logInfo "process terminated (Name: ${process.name}, ID: ${process.id})"
    }

    @Override
    void onProcessPending(TaskHandler handler, TraceRecord trace) {
        logInfo "process submitted to queue of pending tasks to be scheduled (Name: ${handler.task.name}, ID: ${handler.task.id})"
        logTaskHandler(handler)
        logTraceRecord(trace)
    }

    @Override
    void onProcessSubmit(TaskHandler handler, TraceRecord trace) {
        logInfo "process submitted (Name: ${handler.task.name}, ID: ${handler.task.id})"
        logTaskHandler(handler)
        logTraceRecord(trace)
        TaskSpecification task = this.wf.touchTaskSpecification(handler.task.id.toString())
        task.name = handler.task.name
    }

    @Override
    void onProcessStart(TaskHandler handler, TraceRecord trace) {
        logInfo "process started (Name: ${handler.task.name}, ID: ${handler.task.id})"
        logTaskHandler(handler)
        logTraceRecord(trace)
    }

    @Override
    void onProcessComplete(TaskHandler handler, TraceRecord trace) {
        logInfo "process completed (Name: ${handler.task.name}, ID: ${handler.task.id})"
        logTaskHandler(handler)
        logTraceRecord(trace)

        TaskExecution task = this.wf.touchTaskExecution(handler.task.id.toString())
        task.runtimeInSeconds = ((String) trace.get('realtime')).toInteger() / 1000
        task.executedAt = Instant.ofEpochMilli((long) trace.get('start')).atZone(ZoneOffset.UTC).format(DateTimeFormatter.ISO_INSTANT)
        //task.coreCount
        task.avgCPU = ((String) trace.get('%cpu')).toFloat()
        task.readBytes = ((String) trace.get('rchar')).toLong()
        task.writtenBytes = ((String) trace.get('wchar')).toLong()
        task.memoryInBytes = ((String) trace.get('rss')).toLong()
        // task.energyInKWh
        // task.avgPowerInW
        // task.priority
        task.commandProgram = trace.get('script')
        task.commandArguments = []

        task.peakVmem = (trace.get('peak_vmem') as String).toLong()
        task.peakRss = (trace.get('peak_rss') as String).toLong()
        task.volCtxt = (trace.get('vol_ctxt') as String).toLong()
        task.invCtxt = (trace.get('inv_ctxt') as String).toLong()

        // if (task.resourceUsageTimeSeries == null) {
        //     task.resourceUsageTimeSeries = [:]
        // }
        // task.resourceUsageTimeSeries.time_us = task.resourceUsageTimeSeries.time_us ?: []
        // task.resourceUsageTimeSeries.mem_bytes = task.resourceUsageTimeSeries.mem_bytes ?: []
        // task.resourceUsageTimeSeries.cpu_user_ticks = task.resourceUsageTimeSeries.cpu_user_ticks ?: []
        // task.resourceUsageTimeSeries.cpu_sys_ticks = task.resourceUsageTimeSeries.cpu_sys_ticks ?: []

        // File deepLogFile = new File(handler.task.workDir.toFile(), 'bblog_deep.log')
        // deepLogFile.withReader { reader ->
        //     reader.readLine() // skip header
        //     reader.eachLine { line ->
        //         line = line.trim()
        //         if (!line) return

        //         def parts = line.split(/\s*,\s*/)
        //         if (parts.size() < 4) return

        //         task.resourceUsageTimeSeries.time_us << parts[0].toLong()
        //         task.resourceUsageTimeSeries.mem_bytes << parts[1].toLong()
        //         task.resourceUsageTimeSeries.cpu_user_ticks << parts[2].toLong()
        //         task.resourceUsageTimeSeries.cpu_sys_ticks << parts[3].toLong()
        //     }
        // }

        String nodeName = null
        File logFile = new File(handler.task.workDir.toFile(), 'bblog.log')
        logFile.eachLine { line ->
            if (line.startsWith('nodeName:')) {
                nodeName = line.split(':', 2)[1].trim()
            }
        }
        MachineSpecification machine = this.wf.touchMachineSpecification(nodeName)
        logFile.eachLine { line ->
            List<String> parts = line.tokenize(':')*.trim()
            if (parts.size() < 2) return
            String key = parts[0]
            String value = parts[1]
            switch (key) {
                case 'system':
                    machine.system = value
                    break
                case 'architecture':
                    machine.architecture = value
                    break
                case 'release':
                    machine.release = value
                    break
                case 'memoryInBytes':
                    machine.memoryInBytes = value.toString().toLong()
                    break
                case 'cpuCoreCount':
                    machine.cpuCoreCount = value.toString().toFloat()
                    break
                case 'cpuSpeedInMHz':
                    machine.cpuSpeedInMHz = value.toString().toFloat()
                    break
                case 'cpuVendor':
                    machine.cpuVendor = value
                    break
                case 'boot-id':
                    machine.bootID = value
                    break
            }
        }

        task.machines = [nodeName]
    }

    @Override
    void onProcessCached(TaskHandler handler, TraceRecord trace) {
        logInfo "process skipped due to result already being cached or stored (Name: ${handler.task.name}, ID: ${handler.task.id})"
        logTaskHandler(handler)
        logTraceRecord(trace)
    }

    @Override
    boolean enableMetrics() { return true }

    @Override
    void onFlowError(TaskHandler handler, TraceRecord trace) {
        logInfo "pipeline failed (Name: ${handler.task.name}, ID: ${handler.task.id})"
        logTaskHandler(handler)
        logTraceRecord(trace)
        log.info logstr
    }

    @Override
    void onFilePublish(Path destination, Path source) {
        logInfo 'output file published'
        logInfo "- source: $source"
        logInfo "- destination: $destination"
    }

    private String timestamp() {
        return new Date().format('yyyy-MM-dd HH:mm:ss.SSS')
    }

    private void logInfo(String msg) {
        String message = "[BigBrother] [${timestamp()}] $msg"
        logstr += message + '\n'
    }

    private List<Path> expandPathsToFiles(List<Path> paths) {
        return paths.collectMany { path ->
            if (Files.isDirectory(path)) {
                Files.walk(path)
                    .filter { p -> Files.isRegularFile(p) }
                    .toList()
            } else if (Files.isRegularFile(path)) {
                [path]
            } else {
                []
            }
        } as List<Path>
    }

}
