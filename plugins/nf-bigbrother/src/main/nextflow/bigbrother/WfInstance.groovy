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

    // Work out parents/children from the files: A is a parent of B when an
    // output file of A shows up as an input file of B.
    void inferDataDependencies() {
        Map<String, List<String>> producers = [:].withDefault { [] }
        taskSpecifications.each { task ->
            (task.outputFiles ?: []).each { file ->
                if (file) {
                    producers[file] << task.id
                }
            }
        }

        Map<String, Set<String>> parentsOf = [:].withDefault { [] as Set }
        Map<String, Set<String>> childrenOf = [:].withDefault { [] as Set }
        taskSpecifications.each { task ->
            (task.inputFiles ?: []).each { file ->
                producers[file].each { producerId ->
                    if (producerId != task.id) {
                        parentsOf[task.id] << producerId
                        childrenOf[producerId] << task.id
                    }
                }
            }
        }

        taskSpecifications.each { task ->
            task.parents = (parentsOf[task.id] as List<String>).sort()
            task.children = (childrenOf[task.id] as List<String>).sort()
        }
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
