package nextflow.bigbrother

import groovy.transform.PackageScope

@PackageScope
class BigBrotherConfig {

    /* groovylint-disable-next-line UnusedMethodParameter */
    BigBrotherConfig(Map map) {
        //def config = map ?: Collections.emptyMap()
    }
    String getPrefix() { return prefix }

}
