package cn.edu.tsinghua.iotdb.queryV2.engine.control;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

public class QueryJobManager {

    /** to store all queryJobs in one query **/
    private static ThreadLocal<Set<Long>> queryJobIds = new ThreadLocal<>();
    private OverflowFileStreamManager overflowFileStreamManager;

    private AtomicLong jobId;

    private QueryJobManager(){
        jobId = new AtomicLong(0L);
        overflowFileStreamManager = OverflowFileStreamManager.getInstance();
    }

    private static class QueryJobManagerHolder {
        private static final QueryJobManager INSTANCE = new QueryJobManager();
    }

    public static QueryJobManager getInstance() {
        return QueryJobManager.QueryJobManagerHolder.INSTANCE;
    }

    public synchronized long addJobForOneQuery() {
        long jobIdCurrent = jobId.incrementAndGet();

        if (queryJobIds.get() == null) {
            queryJobIds.set(new HashSet<>());
        }
        queryJobIds.get().add(jobIdCurrent);

        return jobIdCurrent;
    }

    public void closeOneJobForOneQuery(long jobId) throws IOException {
        if (queryJobIds.get() == null && queryJobIds.get().contains(jobId)) {
            overflowFileStreamManager.closeAll(jobId);
        }
    }

    public void closeAllJobForOneQuery() throws IOException {
        if (queryJobIds.get() != null) {
            for (long jobId : queryJobIds.get()) {
                overflowFileStreamManager.closeAll(jobId);
            }
            queryJobIds.get().clear();
            queryJobIds.remove();
        }
    }

}
