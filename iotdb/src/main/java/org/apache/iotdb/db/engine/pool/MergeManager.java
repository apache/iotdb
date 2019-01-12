package org.apache.iotdb.db.engine.pool;

import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.ProcessorException;
import org.apache.iotdb.db.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.db.concurrent.ThreadName;

import java.util.concurrent.*;

public class MergeManager {

	private ExecutorService pool;
	private int threadCnt;

	private static class InstanceHolder {
		private static MergeManager instance = new MergeManager();
	}

	private MergeManager() {
		IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
		this.threadCnt = config.mergeConcurrentThreads;
		pool = IoTDBThreadPoolFactory.newFixedThreadPool(threadCnt, ThreadName.MERGE_SERVICE.getName());
	}

	static public MergeManager getInstance() {
		return InstanceHolder.instance;
	}

	/**
	 * @throws ProcessorException
	 *             if the pool is not terminated.
	 */
	public void reopen() throws ProcessorException {
		if (!pool.isTerminated())
			throw new ProcessorException("Merge pool is not terminated!");
		IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
		pool = Executors.newFixedThreadPool(config.mergeConcurrentThreads);
	}

	/**
	 * Refuse new merge submits and exit when all RUNNING THREAD in the pool
	 * end.
	 * 
	 * @param block
	 *            if set block to true, this method will wait for timeOut
	 *            milliseconds to close the merge pool. false, return directly.
	 * @param timeOut
	 *            block time out in milliseconds.
	 * @throws ProcessorException
	 *             if timeOut reach or interrupted while waiting to
	 *             exit.
	 */
	public void forceClose(boolean block, long timeOut) throws ProcessorException {
		pool.shutdownNow();
		if (block) {
			try {
				if (!pool.awaitTermination(timeOut, TimeUnit.MILLISECONDS))
					throw new ProcessorException("Merge thread pool doesn't exit after " + timeOut + " ms");
			} catch (InterruptedException e) {
				throw new ProcessorException(
						"Interrupted while waiting merge thread pool to exit. Because " + e.getMessage());
			}
		}
	}

	/**
	 * Block new merge submits and exit when all RUNNING THREADS AND TASKS IN
	 * THE QUEUE end.
	 * 
	 * @param block
	 *            if set to true, this method will wait for timeOut
	 *            milliseconds. false, return directly. False, return directly.
	 * @param timeOut
	 *            block time out in milliseconds.
	 * @throws ProcessorException
	 *             if timeOut is reached or being interrupted while waiting to
	 *             exit.
	 */
	public void close(boolean block, long timeOut) throws ProcessorException {
		pool.shutdown();
		if (block) {
			try {
				if (!pool.awaitTermination(timeOut, TimeUnit.MILLISECONDS))
					throw new ProcessorException("Merge thread pool doesn't exit after " + timeOut + " ms");
			} catch (InterruptedException e) {
				throw new ProcessorException(
						"Interrupted while waiting merge thread pool to exit. Because" + e.getMessage());
			}
		}
	}

	public Future<?> submit(Runnable task) {
		return pool.submit(task);
	}

	public int getActiveCnt() {
		return ((ThreadPoolExecutor) pool).getActiveCount();
	}

	public int getThreadCnt() {
		return threadCnt;
	}
}
