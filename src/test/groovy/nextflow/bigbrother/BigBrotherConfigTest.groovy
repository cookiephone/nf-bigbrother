package nextflow.bigbrother

import spock.lang.Specification

class BigBrotherConfigTest extends Specification {

    def 'falls back to defaults when the config is empty or missing'() {
        when:
        def config = new BigBrotherConfig(opts)

        then:
        config.outputDir == 'bigbrother'
        config.emitDot
        config.prefix == ''

        and: 'the event log is on, being the cheap path'
        config.emitEvents

        and: 'whole-graph snapshots are off, being the quadratic one'
        !config.emitPartials
        config.snapshotEvery == 1

        where:
        opts << [null, [:]]
    }

    def 'reads overrides from the config map'() {
        when:
        def config = new BigBrotherConfig(
            outputDir: 'out', emitPartials: true, emitEvents: false,
            emitDot: false, snapshotEvery: 25, prefix: 'run')

        then:
        config.outputDir == 'out'
        config.emitPartials
        !config.emitEvents
        !config.emitDot
        config.snapshotEvery == 25
        config.prefix == 'run'
    }

    def 'a snapshot stride below one would divide by zero, so it is clamped'() {
        expect:
        new BigBrotherConfig(snapshotEvery: value).snapshotEvery == 1

        where:
        value << [0, -1]
    }
}
