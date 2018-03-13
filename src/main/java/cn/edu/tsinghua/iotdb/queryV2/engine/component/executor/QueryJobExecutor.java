package cn.edu.tsinghua.iotdb.queryV2.engine.component.executor;

import cn.edu.tsinghua.iotdb.queryV2.engine.QueryEngine;
import cn.edu.tsinghua.iotdb.queryV2.engine.component.job.QueryJob;
import cn.edu.tsinghua.iotdb.queryV2.engine.component.job.QueryJobExecutionMessage;
import cn.edu.tsinghua.iotdb.queryV2.engine.impl.QueryEngineImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;

/**
 * Created by zhangjinrui on 2018/1/10.
 */
public abstract class QueryJobExecutor implements Runnable {

    private QueryJob queryJob;
    private QueryEngine queryEngine;

    protected QueryJobExecutor(QueryJob queryJob) {
        this.queryJob = queryJob;
        this.queryEngine = QueryEngineImpl.getInstance();
    }

    public abstract QueryDataSet execute() throws InterruptedException;

    public void run() {
        try {
            QueryDataSet queryDataSet = execute();
            queryEngine.finishJob(queryJob, queryDataSet);
        } catch (InterruptedException e) {
            queryJob.setMessage(new QueryJobExecutionMessage(e.getMessage()));
            queryEngine.terminateJob(queryJob);
        } catch (Exception e) {
            queryJob.setMessage(new QueryJobExecutionMessage("Unexpected Error:" + e.getMessage()));
            queryEngine.terminateJob(queryJob);
        }
    }
}
