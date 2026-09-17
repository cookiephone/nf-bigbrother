package nextflow.bigbrother

import spock.lang.Specification

class BigBrotherConfigTest extends Specification {

    def 'falls back to defaults when the config is empty or missing'() {
        when:
        def config = new BigBrotherConfig(opts)

        then:
        config.outputDir == 'bigbrother'
        config.emitPartials
        config.emitDot
        config.prefix == ''

        where:
        opts << [null, [:]]
    }

    def 'reads overrides from the config map'() {
        when:
        def config = new BigBrotherConfig(
            outputDir: 'out', emitPartials: false, emitDot: false, prefix: 'run')

        then:
        config.outputDir == 'out'
        !config.emitPartials
        !config.emitDot
        config.prefix == 'run'
    }
}
