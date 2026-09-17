package nextflow.bigbrother

import groovy.transform.CompileStatic
import nextflow.plugin.BasePlugin
import nextflow.plugin.Scoped
import org.pf4j.PluginWrapper

@CompileStatic
class BigBrotherPlugin extends BasePlugin {

    BigBrotherPlugin(PluginWrapper wrapper) {
        super(wrapper)
    }
}
