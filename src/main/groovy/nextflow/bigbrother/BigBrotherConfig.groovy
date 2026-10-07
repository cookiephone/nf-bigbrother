package nextflow.bigbrother

import groovy.transform.CompileStatic
import nextflow.config.spec.ConfigOption
import nextflow.config.spec.ConfigScope
import nextflow.config.spec.ScopeName
import nextflow.script.dsl.Description

// The bigbrother{} block from nextflow.config. Declaring it as a ConfigScope is
// what keeps Nextflow 25.10+ from warning "Unrecognized config option" for every
// setting in the block, and is what makes it show up in `nextflow config -spec`.
@ScopeName('bigbrother')
@Description('''
    The `bigbrother` scope configures the physical execution graph snapshots
    written by the nf-bigbrother plugin.
''')
@CompileStatic
class BigBrotherConfig implements ConfigScope {

    @ConfigOption
    @Description('Directory the JSON and DOT snapshots are written to (default: `\'bigbrother\'`).')
    final String outputDir

    @ConfigOption
    @Description('When `true` write a snapshot after every completed task, not just at the end (default: `true`).')
    final boolean emitPartials

    @ConfigOption
    @Description('When `true` write a Graphviz `.dot` beside every JSON snapshot (default: `true`).')
    final boolean emitDot

    @ConfigOption
    @Description('String prepended to every snapshot file name (default: `\'\'`).')
    final String prefix

    // Nextflow builds the scope through the no-arg constructor to read the schema;
    // the observer uses the Map one to read the values an actual run was given.
    BigBrotherConfig() {
        this([:])
    }

    BigBrotherConfig(Map opts) {
        final Map cfg = opts ?: [:]
        this.outputDir = (cfg.outputDir ?: 'bigbrother') as String
        this.emitPartials = cfg.containsKey('emitPartials') ? cfg.emitPartials as boolean : true
        this.emitDot = cfg.containsKey('emitDot') ? cfg.emitDot as boolean : true
        this.prefix = (cfg.prefix ?: '') as String
    }

}
