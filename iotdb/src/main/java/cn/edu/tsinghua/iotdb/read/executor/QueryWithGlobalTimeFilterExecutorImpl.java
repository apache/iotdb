package cn.edu.tsinghua.iotdb.read.executor;

import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.read.QueryDataSourceExecutor;
import cn.edu.tsinghua.iotdb.read.reader.QueryWithOrWithOutFilterReader;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.impl.MergeQueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * IoTDB query executor with  global time filter
 * */
public class QueryWithGlobalTimeFilterExecutorImpl {

    public QueryWithGlobalTimeFilterExecutorImpl() {
    }

    public static QueryDataSet execute(QueryExpression queryExpression) throws IOException, FileNodeManagerException {

        LinkedHashMap<Path, SeriesReader> readersOfSelectedSeries = new LinkedHashMap<>();
        Filter<Long> timeFilter = ((GlobalTimeFilter) queryExpression.getQueryFilter()).getFilter();
        initReadersOfSelectedSeries(readersOfSelectedSeries, queryExpression.getSelectedSeries(), timeFilter);
        return new MergeQueryDataSet(readersOfSelectedSeries);
    }

    private static void initReadersOfSelectedSeries(LinkedHashMap<Path, SeriesReader> readersOfSelectedSeries,
                                             List<Path> selectedSeries, Filter<Long> timeFilter) throws IOException, FileNodeManagerException {
        for (Path path : selectedSeries) {
            SeriesFilter<Long> seriesFilter = new SeriesFilter<Long>(path, timeFilter);
            QueryDataSource queryDataSource = QueryDataSourceExecutor.getQueryDataSource(seriesFilter);
            SeriesReader seriesReader = new QueryWithOrWithOutFilterReader(queryDataSource, seriesFilter);
            readersOfSelectedSeries.put(path, seriesReader);
        }
    }

}
