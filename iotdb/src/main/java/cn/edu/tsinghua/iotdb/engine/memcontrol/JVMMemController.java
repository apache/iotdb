package cn.edu.tsinghua.iotdb.engine.memcontrol;

import cn.edu.tsinghua.iotdb.conf.IoTDBConfig;
import cn.edu.tsinghua.iotdb.conf.IoTDBDescriptor;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class JVMMemController extends BasicMemController {

    private static Logger logger = LoggerFactory.getLogger(JVMMemController.class);

    // memory used by non-data objects, this is used to estimate the memory used by data
    private long nonDataUsage = 0;

    private static class InstanceHolder {
        private static final JVMMemController INSTANCE = new JVMMemController(IoTDBDescriptor.getInstance().getConfig());
    }

    public static JVMMemController getInstance() {
        return InstanceHolder.INSTANCE;
    }

    private JVMMemController(IoTDBConfig config) {
        super(config);
    }

    @Override
    public long getTotalUsage() {
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory() - nonDataUsage;
    }

    @Override
    public UsageLevel getCurrLevel() {
        long memUsage = getTotalUsage();
        if (memUsage < warningThreshold) {
            return UsageLevel.SAFE;
        } else if (memUsage < dangerouseThreshold) {
            return UsageLevel.WARNING;
        } else {
            return UsageLevel.DANGEROUS;
        }
    }

    @Override
    public void clear() {

    }

    @Override
    public void close() {
        super.close();
    }

    @Override
    public UsageLevel reportUse(Object user, long usage) {
        long memUsage = getTotalUsage() + usage;
        if (memUsage < warningThreshold) {
           /* logger.debug("Safe Threshold : {} allocated to {}, total usage {}",
                    MemUtils.bytesCntToStr(usage),
                    user.getClass(),
                    MemUtils.bytesCntToStr(memUsage));*/
            return UsageLevel.SAFE;
        } else if (memUsage < dangerouseThreshold) {
            logger.debug("Warning Threshold : {} allocated to {}, total usage {}",
                    MemUtils.bytesCntToStr(usage),
                    user.getClass(),
                    MemUtils.bytesCntToStr(memUsage));
            return UsageLevel.WARNING;
        } else {
            logger.warn("Memory request from {} is denied, memory usage : {}",
                    user.getClass(),
                    MemUtils.bytesCntToStr(memUsage));
            return UsageLevel.DANGEROUS;
        }
    }

    @Override
    public void reportFree(Object user, long freeSize) {
        logger.info("{} freed from {}, total usage {}", MemUtils.bytesCntToStr(freeSize)
                ,user.getClass()
                , MemUtils.bytesCntToStr(getTotalUsage()));
        System.gc();
    }
}
