/**
 * 
 */

package eu.els.sie.utils.parallel;

import java.util.concurrent.TimeUnit;

public interface ParallelListener<S, C> {

	public void start(int numThreads, final Iterable<S> elements, Long keepAliveTime, Long timeout, TimeUnit unit,
			boolean breakOnError);

	public void startItem(S element);

	public void endItem(OperationStateEnum ose, S element, C result, Exception e);

	public void end();

}
