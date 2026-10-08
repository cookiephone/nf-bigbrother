package nextflow.bigbrother

import nextflow.Session
import nextflow.trace.TraceObserverV2
import spock.lang.Specification

class BigBrotherFactoryTest extends Specification {

    def 'creates a single BigBrotherObserver'() {
        given:
        def factory = new BigBrotherFactory()

        when:
        def result = factory.create(Mock(Session))

        then:
        result.size() == 1
        result.first() instanceof BigBrotherObserver
    }

    def 'the observer is a v2 trace observer'() {
        expect:
        new BigBrotherObserver() instanceof TraceObserverV2
    }

    def 'metrics are requested, so peak_rss and the ctxt counters get collected'() {
        expect:
        new BigBrotherObserver().enableMetrics()
    }
}
