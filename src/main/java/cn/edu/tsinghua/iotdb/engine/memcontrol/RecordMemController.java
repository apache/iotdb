package cn.edu.tsinghua.iotdb.engine.memcontrol;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;
import cn.edu.tsinghua.iotdb.utils.MemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;

/**
 * This class hold global memory usage of MemUsers. This only counts record(tuple) sizes.
 */
public class RecordMemController extends BasicMemController{

    private static Logger logger = LoggerFactory.getLogger(RecordMemController.class);

    // the key is the reference of the memory user, while the value is its memory usage in byte
    private Map<Object, Long> memMap;
    private AtomicLong totalMemUsed;

    private static class InstanceHolder {
        private static final RecordMemController INSTANCE = new RecordMemController(TsfileDBDescriptor.getInstance().getConfig());
    }

    private RecordMemController(TsfileDBConfig config) {
        super(config);
        memMap = new HashMap<>();
        totalMemUsed = new AtomicLong(0);
    }

    public static RecordMemController getInstance() {
        return InstanceHolder.INSTANCE;
    }

    public long getTotalUsage() {
        return totalMemUsed.get();
    }

    public void clear() {
        memMap.clear();
        totalMemUsed.set(0);
    }

    public void close() {
       super.close();
    }

    public UsageLevel getCurrLevel() {
        long memUsage = totalMemUsed.get();
        if (memUsage < warningThreshold) {
            return UsageLevel.SAFE;
        } else if (memUsage < dangerouseThreshold) {
            return UsageLevel.WARNING;
        } else {
            return UsageLevel.DANGEROUS;
        }
    }

    public UsageLevel reportUse(Object user, long usage) {
        Long oldUsage = memMap.get(user);
        if (oldUsage == null)
            oldUsage = 0L;
        long newTotUsage = totalMemUsed.get() + usage;
        // check if the new usage will reach dangerous threshold
        if (newTotUsage < dangerouseThreshold) {
            newTotUsage = totalMemUsed.addAndGet(usage);
            // double check if updating will reach dangerous threshold
            if (newTotUsage < warningThreshold) {
                // still safe, action taken
                memMap.put(user, oldUsage + usage);
                logger.debug("Safe Threshold : {} allocated to {}, it is using {}, total usage {}",
                        MemUtils.bytesCntToStr(usage),
                        user.getClass(),
                        MemUtils.bytesCntToStr(oldUsage + usage), MemUtils.bytesCntToStr(newTotUsage));
                return UsageLevel.SAFE;
            } else if (newTotUsage < dangerouseThreshold) {
                // become warning because competition with other threads, still take the action
                memMap.put(user, oldUsage + usage);
                logger.debug("Warning Threshold : {} allocated to {}, it is using {}, total usage {}",
                        MemUtils.bytesCntToStr(usage),
                        user.getClass(),
                        MemUtils.bytesCntToStr(oldUsage + usage), MemUtils.bytesCntToStr(newTotUsage));
                return UsageLevel.WARNING;
            } else {
                logger.warn("Memory request from {} is denied, memory usage : {}", user.getClass(), MemUtils.bytesCntToStr(newTotUsage));
                // become dangerous because competition with other threads, discard this action
                totalMemUsed.addAndGet(-usage);
                return UsageLevel.DANGEROUS;
            }
        } else {
            logger.warn("Memory request from {} is denied, memory usage : {}", user.getClass(), MemUtils.bytesCntToStr(newTotUsage));
            return UsageLevel.DANGEROUS;
        }
    }

    public void reportFree(Object user, long freeSize) {
        Long usage = memMap.get(user);
        if (usage == null)
            logger.error("Unregistered memory usage from {}", user.getClass());
        else if (freeSize > usage) {
            logger.error("Request to free {} bytes while it only registered {} bytes", freeSize, usage);
            totalMemUsed.addAndGet(-usage);
            memMap.remove(user);
        } else {
            long newTotalMemUsage = totalMemUsed.addAndGet(-freeSize);
            if(usage - freeSize > 0)
                memMap.put(user, usage - freeSize);
            else
                memMap.remove(user);
            logger.info("{} freed from {}, it is using {}, total usage {}", MemUtils.bytesCntToStr(freeSize)
                    ,user.getClass()
                    , MemUtils.bytesCntToStr(usage - freeSize)
                    , MemUtils.bytesCntToStr(newTotalMemUsage));
        }
    }
}
