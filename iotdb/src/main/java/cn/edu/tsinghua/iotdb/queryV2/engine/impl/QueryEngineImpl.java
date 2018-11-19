package cn.edu.tsinghua.iotdb.queryV2.engine.impl;

import cn.edu.tsinghua.iotdb.concurrent.IoTDBThreadPoolFactory;
import cn.edu.tsinghua.iotdb.queryV2.engine.QueryEngine;
import cn.edu.tsinghua.iotdb.queryV2.engine.QueryJobDispatcher;
import cn.edu.tsinghua.iotdb.queryV2.engine.component.executor.QueryJobExecutor;
import cn.edu.tsinghua.iotdb.queryV2.engine.component.job.*;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

/**
 * Created by zhangjinrui on 2018/1/9.
 */
public class QueryEngineImpl implements QueryEngine, Runnable {
    private static final Logger logger = LoggerFactory.getLogger(QueryEngineImpl.class);
    private static final int PENDING_QUEUE_SIZE = 100000;
    private static final int THREAD_POOL_SIZE = 50;
    private static final String THREAD_POOL_NAME = "QueryEngine";
    private BlockingQueue<QueryJob> queryJobPendingQueue;
    private QueryJobDispatcher queryJobDispatcher;
    private ConcurrentHashMap<QueryJob, QueryDataSet> queryJobResultSet;
    private ExecutorService queryJobExecutorService;

    private QueryEngineImpl() {
        queryJobPendingQueue = new ArrayBlockingQueue<>(PENDING_QUEUE_SIZE);
        queryJobExecutorService = IoTDBThreadPoolFactory.newFixedThreadPool(THREAD_POOL_SIZE, THREAD_POOL_NAME);
        queryJobResultSet = new ConcurrentHashMap<>();
    }

    private static class QueryEngineImplHelper {
        private static final QueryEngineImpl INSTANCE = new QueryEngineImpl();
    }

    @Override
    public QueryJobFuture submit(QueryJob job) throws InterruptedException {
        job.setStatus(QueryJobStatus.PENDING);
        QueryJobFuture queryJobFuture = new QueryJobFutureImpl(job);
        putJobToQueue(job);
        return queryJobFuture;
    }

    @Override
    public void run() {
        while (true) {
            QueryJob queryJob = null;
            try {
                queryJob = queryJobPendingQueue.take();
                checkoutJobStatus(queryJob);
                prepareJob(queryJob);
                executeJob(queryJob);
            } catch (InterruptedException e) {
                logger.info("QueryJob[{}] was terminated", queryJob);
                synchronized (queryJob) {
                    queryJob.setStatus(QueryJobStatus.TERMINATED);
                    queryJob.notify();
                }
            } catch (Exception e) {
                logger.error(String.format("Execute QueryJob[%s] error: ", queryJob), e);
                synchronized (queryJob) {
                    queryJob.setMessage(new QueryJobExecutionMessage(e.getMessage()));
                    queryJob.setStatus(QueryJobStatus.TERMINATED);
                    queryJob.notify();
                }
            }
        }
    }

    private void putJobToQueue(QueryJob queryJob) throws InterruptedException {
        queryJob.setSubmitTimestamp(System.currentTimeMillis());
        queryJobPendingQueue.put(queryJob);
    }

    private void checkoutJobStatus(QueryJob queryJob) throws InterruptedException {
        if (queryJob.getStatus() == QueryJobStatus.WAITING_TO_BE_TERMINATED) {
            throw new InterruptedException("QueryJob[" + queryJob + "] receive Terminating command");
        }
    }

    private void prepareJob(QueryJob queryJob) {
        queryJob.setStatus(QueryJobStatus.READY);
    }

    private void executeJob(QueryJob queryJob) {
        queryJob.setStartTimestamp(System.currentTimeMillis());
        QueryJobExecutor queryJobExecutor = queryJobDispatcher.dispatch(queryJob);
        queryJobExecutorService.submit(queryJobExecutor);
    }

    public void finishJob(QueryJob queryJob, QueryDataSet queryDataSet) {
        synchronized (queryJob) {
            try {
                setQueryDataSet(queryJob, queryDataSet);
                queryJob.setStatus(QueryJobStatus.FINISHED);
                queryJob.setEndTimestamp(System.currentTimeMillis());
                queryJob.notify();
            } catch (Exception e) {
                logger.error(String.format("finish QueryJob[%s] error: ", queryJob), e);
                queryJob.setMessage(new QueryJobExecutionMessage(e.getMessage()));
                queryJob.notify();
            }
        }
    }

    public void terminateJob(QueryJob queryJob) {
        synchronized (queryJob) {
            try {
                queryJob.setStatus(QueryJobStatus.TERMINATED);
                queryJob.notify();
            } catch (Exception e) {
                logger.error(String.format("terminate QueryJob[%s] error: ", queryJob), e);
                queryJob.setMessage(new QueryJobExecutionMessage(e.getMessage()));
                queryJob.notify();
            }
        }
    }

    @Override
    public QueryDataSet retrieveQueryDataSet(QueryJob queryJob) {
        return queryJobResultSet.get(queryJob);
    }

    private void setQueryDataSet(QueryJob job, QueryDataSet queryDataSet) {
        this.queryJobResultSet.put(job, queryDataSet);
    }

    public static QueryEngineImpl getInstance() {
        return QueryEngineImplHelper.INSTANCE;
    }

    public void setQueryJobDispatcher(QueryJobDispatcher queryJobDispatcher) {
        this.queryJobDispatcher = queryJobDispatcher;
    }
}
