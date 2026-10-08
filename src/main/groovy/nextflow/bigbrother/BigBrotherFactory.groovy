package nextflow.bigbrother

import groovy.transform.CompileStatic
import nextflow.Session
import nextflow.trace.TraceObserverFactoryV2
import nextflow.trace.TraceObserverV2

@CompileStatic
class BigBrotherFactory implements TraceObserverFactoryV2 {

    @Override
    Collection<TraceObserverV2> create(Session session) {
        final List<TraceObserverV2> result = new ArrayList<>()
        result.add( new BigBrotherObserver() )
        return result
    }
}
