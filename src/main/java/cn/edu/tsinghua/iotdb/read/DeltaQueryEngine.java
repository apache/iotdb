package cn.edu.tsinghua.iotdb.read;

import cn.edu.tsinghua.iotdb.read.executor.DeltaQueryWithGlobalTimeFilterExecutorImpl;
import cn.edu.tsinghua.iotdb.read.executor.DeltaQueryWithQueryFilterExecutorImpl;
import cn.edu.tsinghua.iotdb.read.executor.DeltaQueryWithoutFilterExecutorImpl;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.exception.QueryFilterOptimizationException;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.util.QueryFilterOptimizer;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExecutor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class DeltaQueryEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeltaQueryEngine.class);
    private QueryExecutor queryExecutor;

    public DeltaQueryEngine() {}

    public QueryDataSet query(QueryExpression queryExpression) throws IOException {
        if (queryExpression.hasQueryFilter()) {
            try {
                QueryFilter queryFilter = queryExpression.getQueryFilter();
                QueryFilter regularQueryFilter = QueryFilterOptimizer.getInstance().convertGlobalTimeFilter(queryFilter, queryExpression.getSelectedSeries());
                queryExpression.setQueryFilter(regularQueryFilter);
                if (regularQueryFilter instanceof GlobalTimeFilter) {
                    return new DeltaQueryWithGlobalTimeFilterExecutorImpl().execute(queryExpression);
                } else {
                    return new DeltaQueryWithQueryFilterExecutorImpl().execute(queryExpression);
                }
            } catch (QueryFilterOptimizationException e) {
                throw new IOException(e);
            }
        } else {
            return new DeltaQueryWithoutFilterExecutorImpl().execute(queryExpression);
        }
    }
}
