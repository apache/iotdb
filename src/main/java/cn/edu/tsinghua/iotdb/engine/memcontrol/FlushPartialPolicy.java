package cn.edu.tsinghua.iotdb.engine.memcontrol;

import cn.edu.tsinghua.iotdb.concurrent.ThreadName;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This class only gives a hint to FilenodeManager that it may flush some data to avoid rush hour.
 */
public class FlushPartialPolicy implements Policy{
    private static final Logger logger = LoggerFactory.getLogger(FlushPartialPolicy.class);
    private Thread workerThread;
    private long sleepInterval = TsfileDBDescriptor.getInstance().getConfig().smallFlushInterval;

    @Override
    public void execute() {
        logger.debug("Memory reaches {}, current memory size is {}, JVM memory is {}, flushing.",
                BasicMemController.getInstance().getCurrLevel(),
                MemUtils.bytesCntToStr(BasicMemController.getInstance().getTotalUsage()),
                MemUtils.bytesCntToStr(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
        // use a thread to avoid blocking
        if (workerThread == null) {
            workerThread = createWorkerThread();
            workerThread.start();
        } else {
            if (workerThread.isAlive()) {
                logger.debug("Last flush is ongoing...");
            } else {
                workerThread = createWorkerThread();
                workerThread.start();
            }
        }
    }

    private Thread createWorkerThread() {
        return new Thread(() -> {
            FileNodeManager.getInstance().forceFlush(BasicMemController.UsageLevel.SAFE);
            try {
                Thread.sleep(sleepInterval);
            } catch (InterruptedException ignored) {
            }
        },ThreadName.FLUSH_PARTIAL_POLICY.getName());
    }
}
