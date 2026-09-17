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

    def 'toJson produces the WfCommons-style structure'() {
        given:
        def wf = new WfInstance()
        wf.name = 'demo'
        wf.schemaVersion = '1.5'
        wf.touchTaskSpecification('1').name = 'P:A'

        when:
        def json = new JsonSlurper().parseText(wf.toJson())

        then:
        json.name == 'demo'
        json.schemaVersion == '1.5'
        json.workflow.specification.tasks.size() == 1
        json.workflow.specification.containsKey('files')
        json.workflow.execution.containsKey('tasks')
        json.workflow.execution.containsKey('machines')
    }
}
