package nextflow.bigbrother

import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import nextflow.Session
import nextflow.plugin.extension.PluginExtensionPoint

@Slf4j
@CompileStatic
class BigbrotherExtension extends PluginExtensionPoint {

    /*
     * A session hold information about current execution of the script
     */
    private Session session

    /*
     * A Custom config extracted from nextflow.config under bigbrother tag
     */
     private BigBrotherConfig config

    /*
     * nf-core initializes the plugin once loaded and session is ready
     * @param session
     */
    @Override
    protected void init(Session session) {
        this.session = session
        this.config = new BigBrotherConfig(session.config.navigate('bigbrother') as Map)
    }
}
