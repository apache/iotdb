package cn.edu.tsinghua.iotdb.read;

import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.read.executor.QueryWithGlobalTimeFilterExecutorImpl;
import cn.edu.tsinghua.iotdb.read.executor.QueryWithFilterExecutorImpl;
import cn.edu.tsinghua.iotdb.read.executor.QueryWithoutFilterExecutorImpl;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.exception.QueryFilterOptimizationException;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilterType;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.util.QueryFilterOptimizer;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class QueryEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryEngine.class);

    public QueryDataSet query(QueryExpression queryExpression) throws IOException, FileNodeManagerException {
        if (queryExpression.hasQueryFilter()) {
            try {
                QueryFilter queryFilter = queryExpression.getQueryFilter();
                QueryFilter regularQueryFilter = QueryFilterOptimizer.getInstance().convertGlobalTimeFilter(queryFilter, queryExpression.getSelectedSeries());
                queryExpression.setQueryFilter(regularQueryFilter);
                if (regularQueryFilter.getType() == QueryFilterType.GLOBAL_TIME) {
                    return QueryWithGlobalTimeFilterExecutorImpl.execute(queryExpression);
                } else {
                    return QueryWithFilterExecutorImpl.execute(queryExpression);
                }
            } catch (QueryFilterOptimizationException e) {
                throw new IOException(e);
            }
        } else {
            return QueryWithoutFilterExecutorImpl.execute(queryExpression);
        }
    }
}
