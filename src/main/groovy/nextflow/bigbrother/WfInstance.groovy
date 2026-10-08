package nextflow.bigbrother

import groovy.json.JsonOutput

/**
 * The workflow instance we build up as the run goes and write out as JSON
 * (WfCommons-style spec + execution trace) or as a Graphviz physical graph.
 * The observer "touches" tasks, files and machines as events come in — created
 * the first time, reused after that.
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

    // What the task asked the scheduler for, as opposed to what it used above.
    // The gap between the two is the thing resource-allocation studies measure.
    int requestedCpus = 0
    long requestedMemoryBytes = 0
    long requestedDiskBytes = 0
    long requestedTimeMillis = 0

    // Lifecycle timestamps. `executedAt` above is the start; these bracket it so
    // the time a task spent waiting is separable from the time it spent running.
    String pendingAt = ''
    String submittedAt = ''
    String completedAt = ''
    // Null rather than zero when submit or start is unknown: on a local executor
    // the wait genuinely is ~0, and a study has to tell those two cases apart.
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
            // Nested under one key so the surrounding record stays a valid
            // WfFormat task execution regardless of what we add here.
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

    String makespanInSeconds = ''
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

    // Which task produced or consumed each file, and the task-level edges that
    // follow. Kept up to date as tasks arrive so a completing task's edges cost
    // only the files it touched, rather than a rescan of the whole run.
    private final Map<String, Set<String>> producersOf = [:]
    private final Map<String, Set<String>> consumersOf = [:]
    private final Map<String, Set<String>> parentsOf = [:]
    private final Map<String, Set<String>> childrenOf = [:]

    // Register one task's files and link it to whatever it shares them with: A
    // is a parent of B when an output file of A is an input file of B. Both
    // directions are handled because a file's consumer can be registered before
    // its producer. Idempotent, so re-registering a task is harmless.
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

    // Copy the edges back onto the task specs, which is where they get
    // serialised from. Cheap: one pass over the tasks, no file scanning.
    void materializeEdges() {
        taskSpecifications.each { task ->
            task.parents = ((parentsOf[task.id] ?: [] as Set<String>) as List<String>).sort()
            task.children = ((childrenOf[task.id] ?: [] as Set<String>) as List<String>).sort()
        }
    }

    // Rebuild every edge from scratch off the task specs. The incremental path
    // above keeps this unnecessary during a run, but it is what makes the model
    // correct for a spec that was populated directly rather than through
    // linkTask, and it is cheap enough at snapshot time.
    void inferDataDependencies() {
        producersOf.clear()
        consumersOf.clear()
        parentsOf.clear()
        childrenOf.clear()
        taskSpecifications.each { task -> linkTask(task.id, task.inputFiles, task.outputFiles) }
        materializeEdges()
    }

    // One task as a self-contained line for the append-only event log. Children
    // are deliberately absent: they are not known when a task finishes, and a
    // reader replaying the log derives every edge from the file lists anyway.
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

    // Header line: everything about the run that is known before any task runs.
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

    // Terminal line. The file table rides along here rather than on every task
    // line, where the sizes would be repeated once per task that touched them.
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

    Map toMap() {
        inferDataDependencies()
        return [
            name          : name,
            description   : description,
            createdAt     : createdAt,
            schemaVersion : schemaVersion,
            author        : [
                name        : '',
                email       : '',
                institution : '',
                country     : '',
            ],
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
                execution     : [
                    makespanInSeconds : makespanInSeconds,
                    executedAt        : executedAt,
                    tasks             : taskExecutions*.toMap(),
                    machines          : machineSpecifications*.toMap(),
                ],
            ],
        ]
    }

    String toJson() {
        return JsonOutput.prettyPrint(JsonOutput.toJson(this.toMap()))
    }

}
