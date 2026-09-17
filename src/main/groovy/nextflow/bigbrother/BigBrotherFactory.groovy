package nextflow.bigbrother

import groovy.transform.CompileStatic
import nextflow.Session
import nextflow.trace.TraceObserver
import nextflow.trace.TraceObserverFactory

@CompileStatic
class BigBrotherFactory implements TraceObserverFactory {

    @Override
    Collection<TraceObserver> create(Session session) {
        final result = new ArrayList()
        result.add( new BigBrotherObserver() )
        return result
    }
}
