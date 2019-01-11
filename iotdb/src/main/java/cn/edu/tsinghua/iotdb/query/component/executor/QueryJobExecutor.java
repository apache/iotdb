package cn.edu.tsinghua.iotdb.query.component.executor;

import cn.edu.tsinghua.iotdb.query.component.job.QueryJob;
import cn.edu.tsinghua.iotdb.query.component.job.QueryJobExecutionMessage;
import cn.edu.tsinghua.iotdb.query.component.job.QueryEngineImpl;
import cn.edu.tsinghua.tsfile.read.query.dataset.QueryDataSet;
import cn.edu.tsinghua.iotdb.query.component.job.QueryEngine;

public abstract class QueryJobExecutor implements Runnable {

    private QueryJob queryJob;
    private QueryEngine queryEngine;

    protected QueryJobExecutor(QueryJob queryJob) {
        this.queryJob = queryJob;
        this.queryEngine = QueryEngineImpl.getInstance();
    }

    public abstract QueryDataSet execute() throws InterruptedException;

    @Override
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
