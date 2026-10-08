package nextflow.bigbrother

import groovy.json.JsonSlurper
import spock.lang.Specification

class WfInstanceTest extends Specification {

    def 'infers a parent/child edge from a shared file'() {
        given:
        def wf = new WfInstance()
        def a = wf.touchTaskSpecification('1')
        a.outputFiles = ['/work/a/out.bam']
        def b = wf.touchTaskSpecification('2')
        b.inputFiles = ['/work/a/out.bam']

        when:
        wf.inferDataDependencies()

        then:
        a.children == ['2']
        a.parents == []
        b.parents == ['1']
        b.children == []
    }

    def 'handles fan-out and fan-in'() {
        given:
        def wf = new WfInstance()
        wf.touchTaskSpecification('1').outputFiles = ['/ref']
        ['2', '3'].each { id ->
            def s = wf.touchTaskSpecification(id)
            s.inputFiles = ['/ref']
            s.outputFiles = ["/out-${id}".toString()]
        }
        wf.touchTaskSpecification('4').inputFiles = ['/out-2', '/out-3']

        when:
        wf.inferDataDependencies()

        then:
        wf.taskSpecifications.find { it.id == '1' }.children == ['2', '3']
        wf.taskSpecifications.find { it.id == '4' }.parents == ['2', '3']
    }

    def 'a task is never its own parent'() {
        given:
        def wf = new WfInstance()
        def t = wf.touchTaskSpecification('1')
        t.inputFiles = ['/shared']
        t.outputFiles = ['/shared']

        when:
        wf.inferDataDependencies()

        then:
        t.parents == []
        t.children == []
    }

    def 'shortName drops the qualified prefix and the sample suffix'() {
        expect:
        WfInstance.shortName('NFCORE:RNASEQ:ALIGN (sample1)') == 'ALIGN'
        WfInstance.shortName('PLAIN') == 'PLAIN'
        WfInstance.shortName('') == ''
    }

    def 'renderPhysicalDot emits nodes and the inferred edge'() {
        given:
        def wf = new WfInstance()
        def a = wf.touchTaskSpecification('1')
        a.name = 'P:A'
        a.outputFiles = ['/f']
        def b = wf.touchTaskSpecification('2')
        b.name = 'P:B'
        b.inputFiles = ['/f']

        when:
        def dot = wf.renderPhysicalDot()

        then:
        dot.contains('digraph physical')
        dot.contains('"t1"')
        dot.contains('"t2"')
        dot.contains('"t1" -> "t2"')
    }

    def 'linkTask builds the same edges incrementally as a full rebuild'() {
        given: 'the validation pipeline shape -- seed fans out, aligns fan in'
        def incremental = new WfInstance()
        def rebuilt = new WfInstance()

        when: 'one instance is linked task by task, as a run would'
        [incremental, rebuilt].each { wf ->
            wf.touchTaskSpecification('1').outputFiles = ['/ref']
            ['2', '3', '4'].each { id ->
                def s = wf.touchTaskSpecification(id)
                s.inputFiles = ['/ref']
                s.outputFiles = ["/out-${id}".toString()]
            }
            wf.touchTaskSpecification('5').inputFiles = ['/out-2', '/out-3', '/out-4']
        }
        incremental.taskSpecifications.each { incremental.linkTask(it.id, it.inputFiles, it.outputFiles) }
        incremental.materializeEdges()
        rebuilt.inferDataDependencies()

        then: 'both agree, edge for edge'
        incremental.taskSpecifications.collect { [it.id, it.parents, it.children] } ==
            rebuilt.taskSpecifications.collect { [it.id, it.parents, it.children] }

        and:
        incremental.taskSpecifications.find { it.id == '1' }.children == ['2', '3', '4']
        incremental.taskSpecifications.find { it.id == '5' }.parents == ['2', '3', '4']
    }

    def 'linkTask links a consumer registered before its producer'() {
        given: 'inputs are registered at submit, outputs only at completion'
        def wf = new WfInstance()
        wf.touchTaskSpecification('2').inputFiles = ['/f']
        wf.touchTaskSpecification('1').outputFiles = ['/f']

        when: 'the consumer is linked first'
        wf.linkTask('2', ['/f'], null)
        wf.linkTask('1', null, ['/f'])
        wf.materializeEdges()

        then: 'the edge still appears'
        wf.taskSpecifications.find { it.id == '1' }.children == ['2']
        wf.taskSpecifications.find { it.id == '2' }.parents == ['1']
    }

    def 'linking a task twice does not duplicate its edges'() {
        given: 'onTaskSubmit then onTaskComplete both register the same inputs'
        def wf = new WfInstance()
        wf.touchTaskSpecification('1').outputFiles = ['/f']
        wf.touchTaskSpecification('2').inputFiles = ['/f']

        when:
        wf.linkTask('1', null, ['/f'])
        wf.linkTask('2', ['/f'], null)
        wf.linkTask('2', ['/f'], null)
        wf.materializeEdges()

        then:
        wf.taskSpecifications.find { it.id == '2' }.parents == ['1']
        wf.taskSpecifications.find { it.id == '1' }.children == ['2']
    }

    def 'linkTask never makes a task its own parent'() {
        given:
        def wf = new WfInstance()
        wf.touchTaskSpecification('1')

        when: 'a task consumes a file it also produces'
        wf.linkTask('1', ['/shared'], ['/shared'])
        wf.materializeEdges()

        then:
        wf.taskSpecifications[0].parents == []
        wf.taskSpecifications[0].children == []
    }

    def 'a task event carries its own record, with parents but not children'() {
        given:
        def wf = new WfInstance()
        wf.touchTaskSpecification('1').outputFiles = ['/f']
        def b = wf.touchTaskSpecification('2')
        b.name = 'P:B (s1)'
        b.inputFiles = ['/f']
        wf.touchTaskExecution('2').runtimeInSeconds = 1.5f
        wf.linkTask('1', null, ['/f'])
        wf.linkTask('2', ['/f'], null)

        when:
        def event = wf.taskEventMap('2')

        then:
        event.event == 'task'
        event.id == '2'
        event.name == 'P:B (s1)'
        event.parents == ['1']
        event.inputFiles == ['/f']
        event.execution.runtimeInSeconds == 1.5f

        and: 'children are omitted -- they are not knowable when a task finishes'
        !event.containsKey('children')
    }

    def 'the terminal event carries the file table and the makespan'() {
        given:
        def wf = new WfInstance()
        wf.makespanInSeconds = 42.5d
        wf.touchTaskSpecification('1')
        wf.touchFileSpecification('/f', 123L)

        when:
        def event = wf.endEventMap('complete')

        then:
        event.event == 'complete'
        event.makespanInSeconds == 42.5d
        event.taskCount == 1
        event.files == [[id: '/f', sizeInBytes: 123L]]
    }

    def 'task execution keeps the base WfFormat fields at the top level'() {
        given:
        def exec = new TaskExecution(id: '1', runtimeInSeconds: 2.5f, memoryInBytes: 1024L, peakRss: 2048L)

        when:
        def map = exec.toMap()

        then: 'the fields consumed by the bundled tools stay where they were'
        map.id == '1'
        map.runtimeInSeconds == 2.5f
        map.memoryInBytes == 1024L
        map.peak_rss == 2048L
        map.command instanceof Map
    }

    def 'the added fields are nested so the base record stays valid WfFormat'() {
        given:
        def exec = new TaskExecution(
            id: '1',
            requestedCpus: 12,
            requestedMemoryBytes: 77309411328L,   // 72 GB, i.e. process_high
            peakRss: 3543348020L,                 // ~3.3 GB actually used
            queueWaitSeconds: 118.0f,
            attempt: 2,
            status: 'COMPLETED',
            exitStatus: '0',
            queue: 'normal',
            executor: 'slurm',
            processName: 'NFCORE_CHIPSEQ:CHIPSEQ:TRIMGALORE',
            taskHash: '9f/b7ca45')

        when:
        def map = exec.toMap()
        def bb = map.bigbrother

        then: 'requested resources sit beside the measured ones, not mixed in'
        bb.requested.cpus == 12
        bb.requested.memoryInBytes == 77309411328L
        map.peak_rss == 3543348020L

        and: 'the scheduling fields a resource study needs are present'
        bb.timing.queueWaitSeconds == 118.0f
        bb.outcome.attempt == 2
        bb.outcome.status == 'COMPLETED'
        bb.outcome.exitStatus == '0'
        bb.placement.queue == 'normal'
        bb.placement.executor == 'slurm'
        bb.identity.process == 'NFCORE_CHIPSEQ:CHIPSEQ:TRIMGALORE'
        bb.identity.hash == '9f/b7ca45'
    }

    def 'queue wait is null, not zero, when it was never observed'() {
        expect: 'a genuine zero wait on a local executor must stay distinguishable'
        new TaskExecution(id: '1').toMap().bigbrother.timing.queueWaitSeconds == null
    }

    def 'makespan is a number and an unattributed run omits the author block'() {
        given: 'WfFormat types makespan as a number and rejects empty author strings'
        def wf = new WfInstance()
        wf.makespanInSeconds = 12.5d

        when:
        def json = new JsonSlurper().parseText(wf.toJson())

        then:
        json.workflow.execution.makespanInSeconds == 12.5d
        !(json.workflow.execution.makespanInSeconds instanceof String)
        !json.containsKey('author')
    }

    def 'toJson produces the WfCommons-style structure'() {
        given:
        def wf = new WfInstance()
        wf.name = 'demo'
        wf.schemaVersion = '1.6'
        wf.touchTaskSpecification('1').name = 'P:A'

        when:
        def json = new JsonSlurper().parseText(wf.toJson())

        then:
        json.name == 'demo'
        json.schemaVersion == '1.6'
        json.workflow.specification.tasks.size() == 1
        json.workflow.specification.containsKey('files')
        json.workflow.execution.containsKey('tasks')

        and: 'machines is omitted while empty, since WfFormat forbids an empty one'
        !json.workflow.execution.containsKey('machines')
    }

    def 'machines appears once a machine is known'() {
        given:
        def wf = new WfInstance()
        wf.touchMachineSpecification('node01')

        expect:
        new JsonSlurper().parseText(wf.toJson())
            .workflow.execution.machines*.nodeName == ['node01']
    }
}
