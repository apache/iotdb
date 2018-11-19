package cn.edu.tsinghua.iotdb.queryV2;

import cn.edu.tsinghua.iotdb.queryV2.engine.QueryJobDispatcher;
import cn.edu.tsinghua.iotdb.queryV2.engine.component.executor.QueryJobExecutor;
import cn.edu.tsinghua.iotdb.queryV2.engine.component.job.QueryJob;
import cn.edu.tsinghua.iotdb.queryV2.engine.component.job.QueryJob.*;
import cn.edu.tsinghua.iotdb.queryV2.engine.component.job.QueryJobFuture;
import cn.edu.tsinghua.iotdb.queryV2.engine.component.job.QueryJobStatus;
import cn.edu.tsinghua.iotdb.queryV2.engine.impl.QueryEngineImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.RowRecord;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by zhangjinrui on 2018/1/10.
 */
public class QueryEngineTest {

    private static final Logger logger = LoggerFactory.getLogger(QueryEngineTest.class);

    private QueryEngineImpl queryEngine;

    @Before
    public void before() {
        queryEngine = QueryEngineImpl.getInstance();
    }

    private void initQueryEngine(int sleepSeconds) {
        queryEngine.setQueryJobDispatcher(new QueryJobDispatcher() {
            @Override
            public QueryJobExecutor dispatch(QueryJob queryJob) {
                return new QueryJobExecutor(queryJob) {
                    @Override
                    public QueryDataSet execute() throws InterruptedException {
                        logger.debug("Test-> Executing job: [" + queryJob + "]");
                        if (queryJob.getStatus() == QueryJobStatus.WAITING_TO_BE_TERMINATED) {
                            throw new InterruptedException("QueryJob[" + queryJob + "] was terminated");
                        }
                        Thread.sleep(sleepSeconds * 1000);
                        logger.debug("Test-> Executing Job Done: [" + queryJob + "]");
                        return new QueryDataSet() {
                            @Override
                            public boolean hasNext() throws IOException {
                                return false;
                            }

                            @Override
                            public RowRecord next() throws IOException {
                                return null;
                            }
                        };
                    }
                };
            }
        });
        Thread queryEngineThread = new Thread(queryEngine);
        queryEngineThread.start();
    }

    @Test
    public void testProcessEndByFinish() throws InterruptedException {
        initQueryEngine(1);

        QueryJob queryJob = new SelectQueryJob(1000L);
        QueryJobFuture queryJobFuture = queryEngine.submit(queryJob);
        queryJobFuture.waitToFinished();
        Assert.assertEquals(QueryJobStatus.FINISHED, queryJob.getStatus());
    }


    @Test
    public void testProcessManyQueryJobAtTheSameTime() throws InterruptedException {
        initQueryEngine(1);
        int count = 100;
        List<QueryJobFuture> queryJobFutureList = new ArrayList<>();
        for (int i = 0; i < count; i++) {
            QueryJobFuture queryJobFuture = queryEngine.submit(new SelectQueryJob(i));
            queryJobFutureList.add(queryJobFuture);
        }
        for (int i = 0; i < count; i++) {
            QueryJobFuture future = queryJobFutureList.get(i);
            future.waitToFinished();
            Assert.assertEquals(QueryJobStatus.FINISHED, future.getCurrentStatus());
        }
    }

    @Test
    public void testTerminateQueryJob() throws InterruptedException {
        initQueryEngine(3);
        QueryJob queryJob = new SelectQueryJob(1001L);
        QueryJobFuture queryJobFuture = queryEngine.submit(queryJob);
        queryJobFuture.terminateCurrentJob();
        QueryJobStatus status = queryJob.getStatus();
        if (status != QueryJobStatus.FINISHED && status != QueryJobStatus.TERMINATED) {
            Assert.fail();
        }
    }
}
