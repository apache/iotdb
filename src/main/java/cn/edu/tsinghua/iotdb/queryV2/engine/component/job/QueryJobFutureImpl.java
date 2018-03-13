package cn.edu.tsinghua.iotdb.queryV2.engine.component.job;

import cn.edu.tsinghua.iotdb.queryV2.engine.QueryEngine;
import cn.edu.tsinghua.iotdb.queryV2.engine.impl.QueryEngineImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Created by zhangjinrui on 2018/1/9.
 */
public class QueryJobFutureImpl implements QueryJobFuture {

    private static final Logger logger = LoggerFactory.getLogger(QueryJobFutureImpl.class);
    private QueryJob queryJob;

    private QueryEngine queryEngine;

    public QueryJobFutureImpl(QueryJob queryJob) {
        this.queryJob = queryJob;
        this.queryEngine = QueryEngineImpl.getInstance();
    }

    @Override
    public void waitToFinished() throws InterruptedException {
        synchronized (queryJob) {
            if (queryJobIsDone(queryJob)) {
                return;
            } else {
                queryJob.wait();
            }
        }
    }

    @Override
    public void terminateCurrentJob() throws InterruptedException {
        synchronized (queryJob) {
            if (!queryJobIsDone(queryJob)) {
                queryJob.setStatus(QueryJobStatus.WAITING_TO_BE_TERMINATED);
                queryJob.wait();
            }
        }
    }

    @Override
    public QueryJobStatus getCurrentStatus() {
        return queryJob.getStatus();
    }

    @Override
    public QueryDataSet retrieveQueryDataSet() {
        return queryEngine.retrieveQueryDataSet(queryJob);
    }

    private boolean queryJobIsDone(QueryJob queryJob) {
        if (queryJob.getStatus() == QueryJobStatus.FINISHED || queryJob.getStatus() == QueryJobStatus.TERMINATED) {
            return true;
        }
        return false;
    }
}
