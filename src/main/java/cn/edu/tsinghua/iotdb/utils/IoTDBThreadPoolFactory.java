package cn.edu.tsinghua.iotdb.utils;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;

import org.apache.thrift.server.TThreadPoolServer.Args;

/**
 * This class is used to create thread pool which must contain the pool name.
 * 
 * @author liukun
 *
 */
public class IoTDBThreadPoolFactory {

	/**
	 * see
	 * {@link Executors#newFixedThreadPool(int, java.util.concurrent.ThreadFactory)}
	 * 
	 * @param nThreads
	 *            the number of threads in the pool
	 * @param poolName
	 *            the name of thread pool
	 * @return fixed size thread pool
	 */
	public static ExecutorService newFixedThreadPool(int nThreads, String poolName) {
		return Executors.newFixedThreadPool(nThreads, new IoTThreadFactory(poolName));
	}

	/**
	 * see
	 * {@link Executors#newSingleThreadExecutor(java.util.concurrent.ThreadFactory)
	 * 
	 * @param poolName
	 *            the name of thread pool
	 * @return thread pool
	 */
	public static ExecutorService newSingleThreadExecutor(String poolName) {
		return Executors.newSingleThreadExecutor(new IoTThreadFactory(poolName));
	}

	/**
	 * see
	 * {@link Executors#newCachedThreadPool(java.util.concurrent.ThreadFactory)
	 * 
	 * @param poolName
	 *            the name of thread pool
	 * @return thread pool
	 */
	public static ExecutorService newCachedThreadPool(String poolName) {
		return Executors.newCachedThreadPool(new IoTThreadFactory(poolName));
	}

	/**
	 * see
	 * {@link Executors#newSingleThreadExecutor(java.util.concurrent.ThreadFactory)
	 * 
	 * @param poolName
	 * @return scheduled thread pool
	 */
	public static ScheduledExecutorService newSingleThreadScheduledExecutor(String poolName) {
		return Executors.newSingleThreadScheduledExecutor(new IoTThreadFactory(poolName));
	}

	/**
	 * see
	 * {@link Executors#newScheduledThreadPool(int, java.util.concurrent.ThreadFactory)
	 * 
	 * @param corePoolSize
	 *            the number of threads to keep in the pool
	 * @param poolName
	 *            the name of thread pool
	 * @return thread pool
	 */
	public static ScheduledExecutorService newScheduledThreadPool(int corePoolSize, String poolName) {
		return Executors.newScheduledThreadPool(corePoolSize, new IoTThreadFactory(poolName));
	}

	public static ExecutorService createJDBCClientThreadPool(Args args, String poolName) {
		SynchronousQueue<Runnable> executorQueue = new SynchronousQueue<Runnable>();
		return new ThreadPoolExecutor(args.minWorkerThreads, args.maxWorkerThreads, args.stopTimeoutVal,
				args.stopTimeoutUnit, executorQueue, (ThreadFactory) new IoTThreadFactory(poolName));
	}

}
