package org.apache.iotdb.db.query.component.executor;

import org.apache.iotdb.db.query.component.job.QueryJob;
import org.apache.iotdb.db.query.component.job.QueryJobExecutionMessage;
import org.apache.iotdb.db.query.component.job.QueryEngineImpl;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.db.query.component.job.QueryEngine;

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
