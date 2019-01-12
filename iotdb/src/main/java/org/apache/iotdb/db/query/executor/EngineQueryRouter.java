package org.apache.iotdb.db.query.executor;

import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.query.control.OpenedFilePathsManager;
import org.apache.iotdb.db.query.control.QueryTokenManager;
import org.apache.iotdb.tsfile.exception.filter.QueryFilterOptimizationException;
import org.apache.iotdb.tsfile.read.expression.IExpression;
import org.apache.iotdb.tsfile.read.expression.QueryExpression;
import org.apache.iotdb.tsfile.read.expression.util.ExpressionOptimizer;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.PathErrorException;
import org.apache.iotdb.db.query.control.OpenedFilePathsManager;
import org.apache.iotdb.db.query.control.QueryTokenManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.iotdb.tsfile.read.expression.ExpressionType.GLOBAL_TIME;

/**
 * <p> Query entrance class of IoTDB query process.
 * All query clause will be transformed to physical plan, physical plan will be executed by EngineQueryRouter.
 */
public class EngineQueryRouter {

    private static final Logger LOGGER = LoggerFactory.getLogger(EngineQueryRouter.class);

    /**
     * Each unique jdbc request(query, aggregation or others job) has an unique job id.
     * This job id will always be maintained until the request is closed.
     * In each job, the unique file will be only opened once to avoid too many opened files error.
     */
    private AtomicLong jobId = new AtomicLong();

    public QueryDataSet query(QueryExpression queryExpression) throws IOException, FileNodeManagerException {

        long jobId = getNextJobId();
        QueryTokenManager.getInstance().setJobIdForCurrentRequestThread(jobId);
        OpenedFilePathsManager.getInstance().setJobIdForCurrentRequestThread(jobId);

        if (queryExpression.hasQueryFilter()) {
            try {
                IExpression optimizedExpression = ExpressionOptimizer.getInstance().
                        optimize(queryExpression.getExpression(), queryExpression.getSelectedSeries());
                queryExpression.setExpression(optimizedExpression);

                if (optimizedExpression.getType() == GLOBAL_TIME) {
                    EngineExecutorWithoutTimeGenerator engineExecutor = new EngineExecutorWithoutTimeGenerator(jobId, queryExpression);
                    return engineExecutor.executeWithGlobalTimeFilter();
                } else {
                    EngineExecutorWithTimeGenerator engineExecutor = new EngineExecutorWithTimeGenerator(jobId, queryExpression);
                    return engineExecutor.execute();
                }

            } catch (QueryFilterOptimizationException | PathErrorException e) {
                throw new IOException(e);
            }
        } else {
            try {
                EngineExecutorWithoutTimeGenerator engineExecutor = new EngineExecutorWithoutTimeGenerator(jobId, queryExpression);
                return engineExecutor.executeWithoutFilter();
            } catch (PathErrorException e) {
                throw new IOException(e);
            }
        }
    }

    private synchronized long getNextJobId() {
        return jobId.incrementAndGet();
    }
}
