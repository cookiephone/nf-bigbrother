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
    @Description('''
        When `true` write a full snapshot during the run, not just at the end
        (default: `false`). Each snapshot re-serialises the whole graph, so on a
        run of any size this costs time quadratic in the task count; the event
        log records the same information incrementally. Raise `snapshotEvery` to
        thin the snapshots out rather than turning them off entirely.
    ''')
    final boolean emitPartials

    @ConfigOption
    @Description('Write a partial snapshot every N completed tasks, when `emitPartials` is set (default: `1`).')
    final int snapshotEvery

    @ConfigOption
    @Description('''
        When `true` append one JSON line per task to `events_*.jsonl` as the run
        proceeds (default: `true`). This is the cheap path: one line per task
        rather than a whole-graph rewrite, and enough to reconstruct the graph at
        any point in the run.
    ''')
    final boolean emitEvents

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
        this.emitPartials = cfg.containsKey('emitPartials') ? cfg.emitPartials as boolean : false
        this.emitEvents = cfg.containsKey('emitEvents') ? cfg.emitEvents as boolean : true
        this.emitDot = cfg.containsKey('emitDot') ? cfg.emitDot as boolean : true
        this.prefix = (cfg.prefix ?: '') as String
        final int every = cfg.containsKey('snapshotEvery') ? cfg.snapshotEvery as int : 1
        this.snapshotEvery = every > 0 ? every : 1
    }

}
