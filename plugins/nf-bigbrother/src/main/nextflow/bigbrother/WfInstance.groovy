import groovy.json.JsonOutput

class TaskExecution {

    String id = ''
    float runtimeInSeconds = 0.0
    String executedAt = ''
    float coreCount = 0.0
    float avgCPU = 0.0
    long readBytes = 0
    long writtenBytes = 0
    long memoryInBytes = 0
    float energyInKWh = 0.0
    float avgPowerInW = 0.0
    int priority = 0
    String[] machines = []
    String commandProgram = ''
    String[] commandArguments = []

    long peakVmem = 0
    long peakRss = 0
    long volCtxt = 0
    long invCtxt = 0

    // Map<String, List> resourceUsageTimeSeries = [
    //     time_us: [],
    //     mem_bytes: [],
    //     cpu_user_ticks: [],
    //     cpu_sys_ticks: [],
    // ]

    Map toMap() {
        return [
            id : id,
            runtimeInSeconds : runtimeInSeconds,
            executedAt : executedAt,
            // coreCount : coreCount,
            avgCPU : avgCPU,
            readBytes : readBytes,
            writtenBytes : writtenBytes,
            memoryInBytes : memoryInBytes,
            // energyInKWh : energyInKWh,
            // avgPowerInW : avgPowerInW,
            // priority : priority,
            machines : machines,
            peak_vmem : peakVmem,
            peak_rss : peakRss,
            vol_ctxt : volCtxt,
            inv_ctxt : invCtxt,
            command : [
                program : commandProgram,
                arguments : commandArguments
            ],
            // resourceUsageTimeSeries: resourceUsageTimeSeries
        ]
    }

}

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
                vendor     : cpuVendor
            ]
        ]
    }

}

class TaskSpecification {

    String name = ''
    String id = ''
    String[] parents = []   // infer from files and data channels
    String[] children = []  // infer from files and data channels
    String[] inputFiles = []
    String[] outputFiles = []

    Map toMap() {
        return [
            name : name,
            id : id,
            parents : parents,
            children : children,
            inputFiles : inputFiles,
            outputFiles : outputFiles,
        ]
    }

}

class FileSpecification {

    String id = ''
    long sizeInBytes = 0

    Map toMap() {
        return [
            id : id,
            sizeInBytes : sizeInBytes
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

    MachineSpecification touchMachineSpecification(String nodeName) {
        MachineSpecification existing = machineSpecifications.find { spec -> spec.nodeName == nodeName }
        if (existing) {
            return existing
        }
        MachineSpecification newMachine = new MachineSpecification(nodeName: nodeName)
        machineSpecifications << newMachine
        return newMachine
    }

    FileSpecification touchFileSpecification(String id, long sizeInBytes) {
        FileSpecification existing = fileSpecifications.find { spec -> spec.id == id }
        if (existing) {
            return existing
        }
        FileSpecification newFile = new FileSpecification(id: id, sizeInBytes: sizeInBytes)
        fileSpecifications << newFile
        return newFile
    }

    TaskSpecification touchTaskSpecification(String id) {
        TaskSpecification existing = taskSpecifications.find { spec -> spec.id == id }
        if (existing) {
            return existing
        }
        TaskSpecification newTask = new TaskSpecification(id: id)
        taskSpecifications << newTask
        return newTask
    }

    TaskExecution touchTaskExecution(String id) {
        TaskExecution existing = taskExecutions.find { spec -> spec.id == id }
        if (existing) {
            return existing
        }
        TaskExecution newTask = new TaskExecution(id: id)
        taskExecutions << newTask
        return newTask
    }

    void update_children_and_parents() {
        // reset parents/children first
        taskSpecifications.each { t ->
            t.parents = [] as String[]
            t.children = [] as String[]
        }
        // build a map: file -> producers (tasks that list it in outputFiles)
        Map<String, List<TaskSpecification>> producers = [:]
        taskSpecifications.each { task ->
            (task.outputFiles ?: []).each { f ->
                if (!f) return
                if (!producers.containsKey(f)) producers[f] = []
                producers[f] << task
            }
        }
        // build a map: file -> consumers (tasks that list it in inputFiles)
        Map<String, List<TaskSpecification>> consumers = [:]
        taskSpecifications.each { task ->
            (task.inputFiles ?: []).each { f ->
                if (!f) return
                if (!consumers.containsKey(f)) consumers[f] = []
                consumers[f] << task
            }
        }
        // infer parents (producers of my inputs) and children (consumers of my outputs)
        taskSpecifications.each { task ->
            Set<String> parentsSet = [] as Set
            Set<String> childrenSet = [] as Set
            (task.inputFiles ?: []).each { f ->
                def ps = producers[f]
                if (ps) {
                    ps.each { p ->
                        if (p?.id && p.id != task.id) parentsSet << p.id
                    }
                }
            }
            (task.outputFiles ?: []).each { f ->
                def cs = consumers[f]
                if (cs) {
                    cs.each { c ->
                        if (c?.id && c.id != task.id) childrenSet << c.id
                    }
                }
            }
            task.parents = parentsSet.toArray(new String[0])
            task.children = childrenSet.toArray(new String[0])
        }
        // enforce symmetry: if A lists B as child, make sure B lists A as parent (and vice versa)
        taskSpecifications.each { task ->
            (task.children ?: []).each { cid ->
                def child = taskSpecifications.find { it.id == cid }
                if (child) {
                    def pset = (child.parents ?: []) as List
                    if (!pset.contains(task.id)) {
                        pset << task.id
                        child.parents = (pset as Set).toArray(new String[0])
                    }
                }
            }
            (task.parents ?: []).each { pid ->
                def parent = taskSpecifications.find { it.id == pid }
                if (parent) {
                    def cset = (parent.children ?: []) as List
                    if (!cset.contains(task.id)) {
                        cset << task.id
                        parent.children = (cset as Set).toArray(new String[0])
                    }
                }
            }
        }
    }

    Map toMap() {
        update_children_and_parents()
        return [
            name : name,
            description : description,
            createdAt : createdAt,
            schemaVersion : schemaVersion,
            author : [
                name : '',
                email : '',
                institution : '',
                country : ''
            ],
            runtimeSystem: [
                name : runtimeSystemName,
                url : runtimeSystemUrl,
                version : runtimeSystemVersion
            ],
            workflow: [
                specification : [
                    tasks : taskSpecifications*.toMap(),
                    files : fileSpecifications*.toMap()
                ],
                execution : [
                    makespanInSeconds : makespanInSeconds,
                    executedAt : executedAt,
                    tasks : taskExecutions*.toMap(),
                    machines : machineSpecifications*.toMap()
                ]
            ]
        ]
    }

    String toJson() {
        return JsonOutput.prettyPrint(JsonOutput.toJson(this.toMap()))
    }

}
