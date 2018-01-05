package cn.edu.tsinghua.iotdb.engine.memcontrol;

import cn.edu.tsinghua.iotdb.conf.TsfileDBConfig;
import cn.edu.tsinghua.iotdb.conf.TsfileDBDescriptor;

public abstract class BasicMemController {

    public enum CONTROLLER_TYPE {
        RECORD, JVM
    }

    protected long warningThreshold;
    protected long dangerouseThreshold;

    protected MemMonitorThread monitorThread;
    protected MemStatisticThread memStatisticThread;

    public enum UsageLevel {
        SAFE, WARNING, DANGEROUS
    }

    BasicMemController(TsfileDBConfig config) {
        warningThreshold = config.memThresholdWarning;
        dangerouseThreshold = config.memThresholdDangerous;
        if(config.enableMemMonitor) {
            monitorThread = new MemMonitorThread(config);
            monitorThread.start();
            memStatisticThread = new MemStatisticThread();
            memStatisticThread.start();
        }
    }

    // change instance here
    public static BasicMemController getInstance() {
        switch (CONTROLLER_TYPE.values()[TsfileDBDescriptor.getInstance().getConfig().memControllerType]) {
            case JVM:
                return JVMMemController.getInstance();
            case RECORD:
            default:
                return RecordMemController.getInstance();
        }
    }

    public void setDangerouseThreshold(long dangerouseThreshold) {
        this.dangerouseThreshold = dangerouseThreshold;
    }

    public void setWarningThreshold(long warningThreshold) {
        this.warningThreshold = warningThreshold;
    }

    public void setCheckInterval(long checkInterval) {
        if(this.monitorThread != null)
            this.monitorThread.setCheckInterval(checkInterval);
    }

    public abstract long getTotalUsage();

    public abstract UsageLevel getCurrLevel();

    public abstract void clear();

    public void close() {
        monitorThread.interrupt();
        memStatisticThread.interrupt();
    }

    public abstract UsageLevel reportUse(Object user, long usage);

    public abstract void reportFree(Object user, long freeSize);
}
