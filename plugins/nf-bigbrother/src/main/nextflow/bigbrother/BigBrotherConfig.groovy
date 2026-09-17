package nextflow.bigbrother

import groovy.transform.CompileStatic
import groovy.transform.PackageScope

// The bigbrother{} block from nextflow.config. See example_nextflow.config for
// the full set of settings and their defaults.
@PackageScope
@CompileStatic
class BigBrotherConfig {

    final String outputDir
    final boolean emitPartials
    final boolean emitDot
    final String prefix

    BigBrotherConfig(Map opts) {
        final Map cfg = opts ?: [:]
        this.outputDir = (cfg.outputDir ?: 'bigbrother') as String
        this.emitPartials = cfg.containsKey('emitPartials') ? cfg.emitPartials as boolean : true
        this.emitDot = cfg.containsKey('emitDot') ? cfg.emitDot as boolean : true
        this.prefix = (cfg.prefix ?: '') as String
    }

}
