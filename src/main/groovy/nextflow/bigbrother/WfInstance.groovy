package nextflow.bigbrother

import groovy.json.JsonOutput

/**
 * The workflow instance we build up as the run goes and write out as JSON
 * (WfCommons-style spec + execution trace) or as a Graphviz physical graph.
 * The observer "touches" tasks, files and machines as events come in, created
 * the first time and reused after that.
 */

/** Runtime measurements of a single task instance. */
class TaskExecution {

    String id = ''
    float runtimeInSeconds = 0.0
    String executedAt = ''
    float avgCPU = 0.0
    long readBytes = 0
    long writtenBytes = 0
    long memoryInBytes = 0
    String[] machines = []
    String commandProgram = ''
    String[] commandArguments = []

    long peakVmem = 0
    long peakRss = 0
    long volCtxt = 0
    long invCtxt = 0

    int requestedCpus = 0
    long requestedMemoryBytes = 0
    long requestedDiskBytes = 0
    long requestedTimeMillis = 0

    String pendingAt = ''
    String submittedAt = ''
    String completedAt = ''
    // Null when submit or start is missing. A local executor really does wait
    // ~0s, so an unknown wait has to stay distinct from a zero one.
    Float queueWaitSeconds = null
    float durationSeconds = 0.0

    int attempt = 0
    String exitStatus = ''
    String status = ''
    String errorAction = ''

    String queue = ''
    String executor = ''
    String container = ''
    String cpuModel = ''
    String hostname = ''
    String nativeId = ''

    String processName = ''
    String tag = ''
    String taskHash = ''

    Map toMap() {
        return [
            id               : id,
            runtimeInSeconds : runtimeInSeconds,
            executedAt       : executedAt,
            avgCPU           : avgCPU,
            readBytes        : readBytes,
            writtenBytes     : writtenBytes,
            memoryInBytes    : memoryInBytes,
            machines         : machines,
            peak_vmem        : peakVmem,
            peak_rss         : peakRss,
            vol_ctxt         : volCtxt,
            inv_ctxt         : invCtxt,
            command          : [
                program   : commandProgram,
                arguments : commandArguments,
            ],
            // Nested so the surrounding record stays valid WfFormat whatever
            // gets added here.
            bigbrother       : [
                requested : [
                    cpus          : requestedCpus,
                    memoryInBytes : requestedMemoryBytes,
                    diskInBytes   : requestedDiskBytes,
                    timeInMillis  : requestedTimeMillis,
                ],
                timing    : [
                    pendingAt        : pendingAt,
                    submittedAt      : submittedAt,
                    completedAt      : completedAt,
                    queueWaitSeconds : queueWaitSeconds,
                    durationSeconds  : durationSeconds,
                ],
                outcome   : [
                    attempt     : attempt,
                    exitStatus  : exitStatus,
                    status      : status,
                    errorAction : errorAction,
                ],
                placement : [
                    queue     : queue,
                    executor  : executor,
                    container : container,
                    cpuModel  : cpuModel,
                    hostname  : hostname,
                    nativeId  : nativeId,
                ],
                identity  : [
                    process : processName,
                    tag     : tag,
                    hash    : taskHash,
                ],
            ],
        ]
    }

}

/** Hardware description of a machine that executed one or more tasks. */
class MachineSpecification {

    String nodeName = ''
    String system = ''
    String architecture = ''
    String release = ''
    long memoryInBytes = 0
    float cpuCoreCount = 0.0
    float cpuSpeedInMHz = 0.0
    String cpuVendor = ''
    String bootID = ''

    Map toMap() {
        return [
            nodeName      : nodeName,
            system        : system,
            architecture  : architecture,
            release       : release,
            memoryInBytes : memoryInBytes,
            bootID        : bootID,
            cpu           : [
                coreCount  : cpuCoreCount,
                speedInMHz : cpuSpeedInMHz,
                vendor     : cpuVendor,
            ],
        ]
    }

}

/** Static description of a task instance and its data dependencies. */
class TaskSpecification {

    String name = ''
    String id = ''
    List<String> parents = []     // inferred from produced/consumed files
    List<String> children = []    // inferred from produced/consumed files
    List<String> inputFiles = []
    List<String> outputFiles = []

    Map toMap() {
        return [
            name        : name,
            id          : id,
            parents     : parents,
            children    : children,
            inputFiles  : inputFiles,
            outputFiles : outputFiles,
        ]
    }

}

/** A file produced or consumed by the workflow. */
class FileSpecification {

    String id = ''
    long sizeInBytes = 0

    Map toMap() {
        return [
            id          : id,
            sizeInBytes : sizeInBytes,
        ]
    }

}

class WfInstance {

    String name = ''
    String description = ''
    String createdAt = ''
    String schemaVersion = ''

    String runtimeSystemName = ''
    String runtimeSystemUrl = ''
    String runtimeSystemVersion = ''

    // A number, not a string: WfFormat types this as `number` and rejects '0'.
    double makespanInSeconds = 0
    String executedAt = ''

    List<TaskSpecification> taskSpecifications = []
    List<FileSpecification> fileSpecifications = []
    List<TaskExecution> taskExecutions = []
    List<MachineSpecification> machineSpecifications = []

    private final Map<String, TaskSpecification> taskSpecIndex = [:]
    private final Map<String, TaskExecution> taskExecIndex = [:]
    private final Map<String, FileSpecification> fileSpecIndex = [:]
    private final Map<String, MachineSpecification> machineIndex = [:]

    MachineSpecification touchMachineSpecification(String nodeName) {
        MachineSpecification existing = machineIndex[nodeName]
        if (existing) {
            return existing
        }
        MachineSpecification machine = new MachineSpecification(nodeName: nodeName)
        machineIndex[nodeName] = machine
        machineSpecifications << machine
        return machine
    }

    FileSpecification touchFileSpecification(String id, long sizeInBytes) {
        FileSpecification existing = fileSpecIndex[id]
        if (existing) {
            return existing
        }
        FileSpecification file = new FileSpecification(id: id, sizeInBytes: sizeInBytes)
        fileSpecIndex[id] = file
        fileSpecifications << file
        return file
    }

    TaskSpecification touchTaskSpecification(String id) {
        TaskSpecification existing = taskSpecIndex[id]
        if (existing) {
            return existing
        }
        TaskSpecification task = new TaskSpecification(id: id)
        taskSpecIndex[id] = task
        taskSpecifications << task
        return task
    }

    TaskExecution touchTaskExecution(String id) {
        TaskExecution existing = taskExecIndex[id]
        if (existing) {
            return existing
        }
        TaskExecution task = new TaskExecution(id: id)
        taskExecIndex[id] = task
        taskExecutions << task
        return task
    }

    // Maintained as tasks arrive, so linking a task costs only the files it
    // touched instead of a rescan of the whole run.
    private final Map<String, Set<String>> producersOf = [:]
    private final Map<String, Set<String>> consumersOf = [:]
    private final Map<String, Set<String>> parentsOf = [:]
    private final Map<String, Set<String>> childrenOf = [:]

    // A is a parent of B when an output file of A is an input file of B. Links
    // in both directions because inputs are registered at submit but outputs
    // only at completion, so a consumer can arrive before its producer.
    // Idempotent, since a task is linked at submit and again at completion.
    void linkTask(String taskId, List<String> inputs, List<String> outputs) {
        (outputs ?: []).each { file ->
            if (!file) {
                return
            }
            producersOf.computeIfAbsent(file) { new LinkedHashSet<String>() }.add(taskId)
            consumersOf[file]?.each { consumer -> addEdge(taskId, consumer) }
        }
        (inputs ?: []).each { file ->
            if (!file) {
                return
            }
            consumersOf.computeIfAbsent(file) { new LinkedHashSet<String>() }.add(taskId)
            producersOf[file]?.each { producer -> addEdge(producer, taskId) }
        }
    }

    private void addEdge(String parent, String child) {
        if (parent == child) {
            return
        }
        parentsOf.computeIfAbsent(child) { new LinkedHashSet<String>() }.add(parent)
        childrenOf.computeIfAbsent(parent) { new LinkedHashSet<String>() }.add(child)
    }

    List<String> parentsOfTask(String taskId) {
        return ((parentsOf[taskId] ?: [] as Set<String>) as List<String>).sort()
    }

    // Copy the edges onto the task specs, which is what gets serialised.
    void materializeEdges() {
        taskSpecifications.each { task ->
            task.parents = ((parentsOf[task.id] ?: [] as Set<String>) as List<String>).sort()
            task.children = ((childrenOf[task.id] ?: [] as Set<String>) as List<String>).sort()
        }
    }

    // Full rebuild from the task specs. Only needed for specs populated
    // directly rather than through linkTask, which is why snapshots still
    // call it.
    void inferDataDependencies() {
        producersOf.clear()
        consumersOf.clear()
        parentsOf.clear()
        childrenOf.clear()
        taskSpecifications.each { task -> linkTask(task.id, task.inputFiles, task.outputFiles) }
        materializeEdges()
    }

    // One line of the event log. Children are absent because they are not
    // known when a task finishes, so a reader derives edges from the files.
    Map taskEventMap(String taskId) {
        final TaskSpecification spec = taskSpecIndex[taskId]
        final TaskExecution exec = taskExecIndex[taskId]
        return [
            event       : 'task',
            id          : taskId,
            name        : spec?.name ?: '',
            parents     : parentsOfTask(taskId),
            inputFiles  : spec?.inputFiles ?: [],
            outputFiles : spec?.outputFiles ?: [],
            execution   : exec?.toMap(),
        ]
    }

    Map runEventMap() {
        return [
            event         : 'run',
            name          : name,
            description   : description,
            createdAt     : createdAt,
            executedAt    : executedAt,
            schemaVersion : schemaVersion,
            runtimeSystem : [
                name    : runtimeSystemName,
                url     : runtimeSystemUrl,
                version : runtimeSystemVersion,
            ],
        ]
    }

    // The file table goes here rather than on each task line, where sizes
    // would repeat once per task that touched the file.
    Map endEventMap(String phase) {
        return [
            event             : phase,
            makespanInSeconds : makespanInSeconds,
            taskCount         : taskSpecifications.size(),
            files             : fileSpecifications*.toMap(),
            machines          : machineSpecifications*.toMap(),
        ]
    }

    // Just the process name, without the qualified prefix or the (sample) suffix.
    static String shortName(String fullName) {
        if (!fullName) {
            return ''
        }
        String base = fullName.replaceAll(/\s*\(.*\)\s*$/, '')
        int idx = base.lastIndexOf(':')
        return idx >= 0 ? base.substring(idx + 1) : base
    }

    // The graph so far as a DOT document: a node per task, edges from the file
    // dependencies, one fill colour per process.
    String renderPhysicalDot() {
        inferDataDependencies()

        List<String> palette = [
            '#8dd3c7', '#ffffb3', '#bebada', '#fb8072', '#80b1d3', '#fdb462',
            '#b3de69', '#fccde5', '#d9d9d9', '#bc80bd', '#ccebc5', '#ffed6f',
        ]
        Map<String, String> colourOf = [:]
        int next = 0

        StringBuilder sb = new StringBuilder()
        sb << 'digraph physical {\n'
        sb << '  rankdir=TB;\n'
        sb << '  node [shape=box, style="rounded,filled", fontname="Helvetica", fontsize=10];\n'
        sb << '  edge [color="#666666"];\n'

        taskSpecifications.each { task ->
            String proc = shortName(task.name) ?: 'task'
            String colour = colourOf[proc]
            if (!colour) {
                colour = palette[next % palette.size()]
                colourOf[proc] = colour
                next++
            }
            String label = "${proc}\\n[${task.id}]"
            sb << "  \"t${task.id}\" [label=\"${dotEscape(label)}\", fillcolor=\"${colour}\"];\n"
        }

        taskSpecifications.each { task ->
            (task.children ?: []).each { child ->
                sb << "  \"t${task.id}\" -> \"t${child}\";\n"
            }
        }

        sb << '}\n'
        return sb.toString()
    }

    private static String dotEscape(String value) {
        return value.replace('\\', '\\\\').replace('"', '\\"')
    }

    // `author` and `machines` are both optional in WfFormat but may not be
    // empty when present, so an unattributed run, or one without the wrapper
    // patch, leaves them out rather than emitting hollow ones.
    Map toMap() {
        inferDataDependencies()
        final Map execution = [
            makespanInSeconds : makespanInSeconds,
            executedAt        : executedAt,
            tasks             : taskExecutions*.toMap(),
        ]
        if (machineSpecifications) {
            execution.machines = machineSpecifications*.toMap()
        }
        return [
            name          : name,
            description   : description,
            createdAt     : createdAt,
            schemaVersion : schemaVersion,
            runtimeSystem : [
                name    : runtimeSystemName,
                url     : runtimeSystemUrl,
                version : runtimeSystemVersion,
            ],
            workflow      : [
                specification : [
                    tasks : taskSpecifications*.toMap(),
                    files : fileSpecifications*.toMap(),
                ],
                execution     : execution,
            ],
        ]
    }

    String toJson() {
        return JsonOutput.prettyPrint(JsonOutput.toJson(this.toMap()))
    }

}
