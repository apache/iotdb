package cn.edu.tsinghua.iotdb.engine.memcontrol;

import cn.edu.tsinghua.iotdb.utils.MemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class NoActPolicy implements Policy {
    private static final Logger logger = LoggerFactory.getLogger(NoActPolicy.class);
    @Override
    public void execute() {
        logger.debug("Memory check is safe, current usage {}, JVM memory {}" ,
                MemUtils.bytesCntToStr(BasicMemController.getInstance().getTotalUsage()),
                MemUtils.bytesCntToStr(Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory()));
    }
}
