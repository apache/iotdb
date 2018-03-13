package cn.edu.tsinghua.iotdb.queryV2.engine;

import cn.edu.tsinghua.iotdb.queryV2.engine.component.job.QueryJob;
import cn.edu.tsinghua.iotdb.queryV2.engine.component.job.QueryJobFuture;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;

/**
 * Created by zhangjinrui on 2018/1/9.
 */
public interface QueryEngine {

    /**
     * Submit a QueryJob to QueryEngine
     *
     * @param job
     * @return QueryJobFuture for submitted QueryJob
     */
    QueryJobFuture submit(QueryJob job) throws InterruptedException;

    void finishJob(QueryJob queryJob, QueryDataSet queryDataSet);

    void terminateJob(QueryJob queryJob);

    /**
     *
     * @param queryJob
     * @return null if there is NOT corresponding QueryDataSet for given queryJob
     */
    QueryDataSet retrieveQueryDataSet(QueryJob queryJob);
}
