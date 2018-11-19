package cn.edu.tsinghua.tsfile.timeseries.readV2.query.impl;

import cn.edu.tsinghua.tsfile.timeseries.filterV2.exception.QueryFilterOptimizationException;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.QueryFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.util.QueryFilterOptimizer;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExecutor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2017/12/27.
 */
public class QueryExecutorRouter implements QueryExecutor {

    private MetadataQuerier metadataQuerier;
    private SeriesChunkLoader seriesChunkLoader;

    public QueryExecutorRouter(MetadataQuerier metadataQuerier, SeriesChunkLoader seriesChunkLoader) {
        this.metadataQuerier = metadataQuerier;
        this.seriesChunkLoader = seriesChunkLoader;
    }

    @Override
    public QueryDataSet execute(QueryExpression queryExpression) throws IOException {
        if (queryExpression.hasQueryFilter()) {
            try {
                QueryFilter queryFilter = queryExpression.getQueryFilter();
                QueryFilter regularQueryFilter = QueryFilterOptimizer.getInstance().convertGlobalTimeFilter(queryFilter, queryExpression.getSelectedSeries());
                queryExpression.setQueryFilter(regularQueryFilter);
                if (regularQueryFilter instanceof GlobalTimeFilter) {
                    return new QueryWithGlobalTimeFilterExecutorImpl(seriesChunkLoader, metadataQuerier).execute(queryExpression);
                } else {
                    return new QueryWithQueryFilterExecutorImpl(seriesChunkLoader, metadataQuerier).execute(queryExpression);
                }
            } catch (QueryFilterOptimizationException e) {
                throw new IOException(e);
            }
        } else {
            return new QueryWithoutFilterExecutorImpl(seriesChunkLoader, metadataQuerier).execute(queryExpression);
        }
    }
}
