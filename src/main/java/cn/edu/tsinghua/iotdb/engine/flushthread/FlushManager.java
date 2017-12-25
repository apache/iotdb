package cn.edu.tsinghua.iotdb.engine.flushthread;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.utils.IoTDBThreadPoolFactory;
import cn.edu.tsinghua.tsfile.common.exception.ProcessorException;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class FlushManager {

    private static final int EXIT_WAIT_TIME = 60 * 1000;

    private ExecutorService pool;

    private static class InstanceHolder {
        private static FlushManager instance = new FlushManager();
    }

    private FlushManager() {
        TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
        pool = IoTDBThreadPoolFactory.newFixedThreadPool(config.concurrentFlushThread, "Flush");
    }

    static public FlushManager getInstance(){
        return InstanceHolder.instance;
    }

    /**
     * @throws ProcessorException if the pool is not terminated.
     */
    public void reopen() throws ProcessorException {
        if(!pool.isTerminated())
            throw new ProcessorException("Pool is not terminated!");
        TsfileDBConfig config = TsfileDBDescriptor.getInstance().getConfig();
        pool = Executors.newFixedThreadPool(config.concurrentFlushThread);
    }

    /**
     * Refuse new flush submits and exit when all RUNNING THREAD in the pool end.
     * @param block if set to true, this method will wait for timeOut milliseconds.
     * @param timeOut block time out in milliseconds.
     * @throws ProcessorException if timeOut is reached or being interrupted while waiting to exit.
     */
    public void forceClose(boolean block, long timeOut) throws ProcessorException {
        pool.shutdownNow();
        if(block) {
            try {
                if(!pool.awaitTermination(timeOut, TimeUnit.MILLISECONDS))
                    throw new ProcessorException("Flush thread pool doesn't exit after " + EXIT_WAIT_TIME + " ms");
            } catch (InterruptedException e) {
               throw new ProcessorException("Interrupted while waiting flush thread pool to exit. " + e.getMessage());
            }
        }
    }

    /**
     * Block new flush submits and exit when all RUNNING THREADS AND TASKS IN THE QUEUE end.
     * @param block if set to true, this method will wait for timeOut milliseconds.
     * @param timeOut block time out in milliseconds.
     * @throws ProcessorException if timeOut is reached or being interrupted while waiting to exit.
     */
    public void close(boolean block, long timeOut) throws ProcessorException {
        pool.shutdown();
        if(block) {
            try {
                if(!pool.awaitTermination(timeOut, TimeUnit.MILLISECONDS))
                    throw new ProcessorException("Flush thread pool doesn't exit after " + EXIT_WAIT_TIME + " ms");
            } catch (InterruptedException e) {
                throw new ProcessorException("Interrupted while waiting flush thread pool to exit. " + e.getMessage());
            }
        }
    }

    synchronized public void submit(Runnable task) {
        pool.execute(task);
    }

}
