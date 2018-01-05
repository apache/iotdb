package cn.edu.tsinghua.iotdb.engine.memcontrol;

import cn.edu.tsinghua.iotdb.utils.MemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class MemStatisticThread extends Thread{

    private static Logger logger = LoggerFactory.getLogger(MemStatisticThread.class);

    // update statistic every such interval
    private long checkInterval = 100; // in ms

    private long minMemUsage = Long.MAX_VALUE;
    private long maxMemUsage = Long.MIN_VALUE;
    private double meanMemUsage = 0.0;
    private long minJVMUsage = Long.MAX_VALUE;
    private long maxJVMUsage = Long.MIN_VALUE;
    private double meanJVMUsage = 0.0;
    private int cnt = 0;
    // log statistic every so many intervals
    private int reportCycle = 60;

    public MemStatisticThread() {
        this.setName("IoTDB-MemStatistic-thread");
    }

    @Override
    public void run() {
        logger.info("MemStatisticThread started");
        try {
            // wait 3 mins for system to setup
            Thread.sleep( 3 * 60 * 1000);
        } catch (InterruptedException e) {
            logger.info("MemStatisticThread exiting...");
            return;
        }
        super.run();
        while (true) {
            if(this.isInterrupted()) {
                logger.info("MemStatisticThread exiting...");
                return;
            }
            long memUsage = BasicMemController.getInstance().getTotalUsage();
            long jvmUsage = Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
            minMemUsage = memUsage < minMemUsage ? memUsage : minMemUsage;
            minJVMUsage = jvmUsage < minJVMUsage ? jvmUsage : minJVMUsage;
            maxMemUsage = memUsage > maxMemUsage ? memUsage : maxMemUsage;
            maxJVMUsage = jvmUsage > maxJVMUsage ? jvmUsage : maxJVMUsage;
            double doubleCnt = new Integer(cnt).doubleValue();
            meanMemUsage = meanMemUsage * (doubleCnt / (doubleCnt + 1.0)) + memUsage / (doubleCnt + 1.0);
            meanJVMUsage = meanJVMUsage * (doubleCnt / (doubleCnt + 1.0)) + jvmUsage / (doubleCnt + 1.0);

            if(++cnt % reportCycle == 0)
                logger.info("Monitored memory usage, min {}, max {}, mean {} \n" +
                        "JVM memory usage, min {}, max {}, mean {}",
                        MemUtils.bytesCntToStr(minMemUsage), MemUtils.bytesCntToStr(maxMemUsage), MemUtils.bytesCntToStr(new Double(meanMemUsage).longValue()),
                        MemUtils.bytesCntToStr(minJVMUsage), MemUtils.bytesCntToStr(maxJVMUsage), MemUtils.bytesCntToStr(new Double(meanJVMUsage).longValue()));
            try {
                Thread.sleep(checkInterval);
            } catch (InterruptedException e) {
                logger.info("MemMonitorThread exiting...");
                return;
            }
        }
    }
}
