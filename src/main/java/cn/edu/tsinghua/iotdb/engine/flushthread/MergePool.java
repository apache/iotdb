package cn.edu.tsinghua.iotdb.engine.flushthread;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.utils.IoTDBThreadPoolFactory;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;

public class MergePool {

	private ExecutorService pool;
	private int threadCnt;

	private static class InstanceHolder {
		private static MergePool instance = new MergePool();
	}

	private MergePool() {
		TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
		this.threadCnt = config.mergeConcurrentThreads;
		pool = IoTDBThreadPoolFactory.newFixedThreadPool(threadCnt, "Merge");
	}

	static public MergePool getInstance() {
		return InstanceHolder.instance;
	}

	/**
	 * @throws ProcessorException
	 *             if the pool is not terminated.
	 */
	public void reopen() throws ProcessorException {
		if (!pool.isTerminated())
			throw new ProcessorException("Merge pool is not terminated!");
		TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
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
