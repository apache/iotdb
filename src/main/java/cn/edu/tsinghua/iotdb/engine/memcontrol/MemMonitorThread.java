package cn.edu.tsinghua.iotdb.engine.memcontrol;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemMonitorThread extends Thread {

    private static Logger logger = LoggerFactory.getLogger(MemMonitorThread.class);

    private long checkInterval = 1000; // in ms

    private Policy safePolicy;
    private Policy warningPolicy;
    private Policy dangerousPolicy;

    public MemMonitorThread(TsfileDBConfig config) {
        this.setName("IoTDB-MemMonitor-thread");
        long checkInterval = config.memMonitorInterval;
        this.checkInterval = checkInterval > 0 ? checkInterval : this.checkInterval;
        if(config.enableSmallFlush)
            this.safePolicy = new FlushPartialPolicy();
        else
            this.safePolicy = new NoActPolicy();
        this.warningPolicy = new ForceFLushAllPolicy();
        this.dangerousPolicy = new ForceFLushAllPolicy();
    }

    public void setCheckInterval(long checkInterval) {
        this.checkInterval = checkInterval;
    }

    @Override
    public void run() {
        logger.info("MemMonitorThread started");
        super.run();
        while (true) {
            if(this.isInterrupted()) {
                logger.info("MemMonitorThread exiting...");
                return;
            }
            BasicMemController.UsageLevel level = BasicMemController.getInstance().getCurrLevel();
            switch (level) {
                case WARNING:
                    warningPolicy.execute();
                    break;
                case DANGEROUS:
                    dangerousPolicy.execute();
                    break;
                case SAFE:
                    safePolicy.execute();
                    break;
                default:
                    logger.error("Unknown usage level : {}", level);
            }
            try {
                Thread.sleep(checkInterval);
            } catch (InterruptedException e) {
                logger.info("MemMonitorThread exiting...");
                return;
            }
        }
    }
}
