
package eu.els.sie.utils.parallel;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

public class Parallel<T, C> {

	private ParallelListener<T, C> listener;

	public Parallel() {
		listener = new ParallelListener<T, C>() {

			@Override
			public void end() {
				// TODO Auto-generated method stub
			}

			@Override
			public void startItem(T element) {
				// TODO Auto-generated method stub

			}

			@Override
			public void endItem(OperationStateEnum ose, T element, C result, Exception th) {
				// TODO Auto-generated method stub

			}

			@Override
			public void start(int numThreads, Iterable<T> elements, Long keepAliveTime, Long timeout, TimeUnit unit,
					boolean breakOnError) {
				// TODO Auto-generated method stub

			}

		};
	}

	public void setListener(ParallelListener<T, C> listener) {
		this.listener = listener;
	}

	private static final int NUM_CORES = Runtime.getRuntime().availableProcessors();

	public void blockingFor(final Iterable<T> elements, final Operation<T, C> operation, final boolean breakOnError) {
		blockingFor(2 * NUM_CORES, elements, operation, breakOnError);
	}

	public void blockingFor(int numThreads, final Iterable<T> elements, final Operation<T, C> operation,
			final boolean breakOnError) {
		asyncFor(numThreads, elements, operation, 0L, Long.MAX_VALUE, TimeUnit.DAYS, breakOnError);
	}

	public void asyncFor(final Iterable<T> elements, final Operation<T, C> operation, final boolean breakOnError) {
		asyncFor(2 * NUM_CORES, elements, operation, breakOnError);
	}

	public void asyncFor(int numThreads, final Iterable<T> elements, final Operation<T, C> operation,
			final boolean breakOnError) {
		asyncFor(numThreads, elements, operation, 0L, Long.MAX_VALUE, TimeUnit.DAYS, breakOnError);
	}

	public void asyncFor(int numThreads, final Iterable<T> elements, final Operation<T, C> operation,
			final Long keepAliveTime, final Long timeout, final TimeUnit unit, final boolean breakOnError) {

		ThreadPoolExecutor threadPoolExecutor = new ThreadPoolExecutor(numThreads, numThreads, keepAliveTime, unit,
				new LinkedBlockingQueue<Runnable>());
		final ThreadSafeIterator<T> itr = new ThreadSafeIterator<T>(elements.iterator());

		listener.start(numThreads, elements, keepAliveTime, timeout, unit, breakOnError);

		LinkedHashMap<T, Future<C>> futureList = new LinkedHashMap<T, Future<C>>();

		T current;
		while ((current = itr.next()) != null) {
			final T element = current;
			Future<C> future = threadPoolExecutor.submit(new Callable<C>() {
				@Override
				public C call() {
					listener.startItem(element);
					try {
						C result = operation.perform(element);
						listener.endItem(OperationStateEnum.SUCCED, element, result, null);
						return result;
					} catch (Exception e) {
						listener.endItem(OperationStateEnum.FAILED, element, null, e);
						if (breakOnError) {
							List<T> remaining = itr.nextAll();
							for (T element : remaining) {
								listener.endItem(OperationStateEnum.INTURRUPTED, element, null, null);
							}
						}

					}
					return null;
				}
			});
			futureList.put(current, future);
		}

		threadPoolExecutor.shutdown();

		for (Entry<T, Future<C>> entry : futureList.entrySet()) {
			try {
				entry.getValue().get(timeout, unit);
			} catch (InterruptedException | ExecutionException | TimeoutException e) {
				listener.endItem(OperationStateEnum.FAILED, entry.getKey(), null, e);
			}
		}

		listener.end();

	}

	private static class ThreadSafeIterator<T> {

		private final Iterator<T> itr;

		public ThreadSafeIterator(Iterator<T> itr) {
			this.itr = itr;
		}

		public synchronized List<T> nextAll() {
			List<T> set = new LinkedList<>();
			while (itr.hasNext()) {
				set.add(itr.next());
			}
			return set;
		}

		public synchronized T next() {
			if (itr.hasNext()) {
				return itr.next();
			}
			return null;
		}

	}

	public static interface Operation<T, C> {
		public C perform(T pParameter) throws Exception;
	}

}