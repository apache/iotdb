package cn.edu.tsinghua.iotdb.queryV2.engine.externalsort;

/**
 * Created by zhangjinrui on 2018/1/20.
 */
public class ExternalSortJobScheduler {

    private long jobId = 0;

    private ExternalSortJobScheduler() {

    }

    public synchronized long genJobId() {
        jobId++;
        return jobId;
    }

    private static class ExternalSortJobSchedulerHelper {
        private static ExternalSortJobScheduler INSTANCE = new ExternalSortJobScheduler();
    }

    public static ExternalSortJobScheduler getInstance() {
        return ExternalSortJobSchedulerHelper.INSTANCE;
    }
}
