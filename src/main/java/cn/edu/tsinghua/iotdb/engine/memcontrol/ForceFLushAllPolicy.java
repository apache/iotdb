package cn.edu.tsinghua.iotdb.engine.memcontrol;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ForceFLushAllPolicy implements Policy {
    private Logger logger = LoggerFactory.getLogger(ForceFLushAllPolicy.class);
    private Thread workerThread;

    @Override
    public void execute() {
        logger.info("Memory reachs {}, current memory size {}, JVM memory {}, flushing.",
                BasicMemController.getInstance().getCurrLevel(),
                MemUtils.bytesCntToStr(BasicMemController.getInstance().getTotalUsage()),
                MemUtils.bytesCntToStr(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
        // use a thread to avoid blocking
        if (workerThread == null) {
            workerThread = new Thread(() -> {
                FileNodeManager.getInstance().forceFlush(BasicMemController.UsageLevel.DANGEROUS);
                System.gc();
            });
            workerThread.start();
        } else {
            if (workerThread.isAlive()) {
                logger.info("Last flush is ongoing...");
            } else {
                workerThread = new Thread(() -> {
                    FileNodeManager.getInstance().forceFlush(BasicMemController.UsageLevel.DANGEROUS);
                    System.gc();
                });
                workerThread.start();
            }
        }
    }
}
