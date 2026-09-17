package nextflow.bigbrother

import nextflow.Session
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
}
