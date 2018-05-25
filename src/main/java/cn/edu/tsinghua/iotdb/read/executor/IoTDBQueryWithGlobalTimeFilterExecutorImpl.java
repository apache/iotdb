package cn.edu.tsinghua.iotdb.read.executor;

import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.read.IoTDBQueryDataSourceExecutor;
import cn.edu.tsinghua.iotdb.read.IoTDBQueryExecutor;
import cn.edu.tsinghua.iotdb.read.reader.IoTDBQueryWithFilterReader;
import cn.edu.tsinghua.iotdb.read.reader.IoTDBQueryWithoutFilterReader;
import cn.edu.tsinghua.iotdb.read.timegenerator.IoTDBTimeGenerator;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.GlobalTimeFilter;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExecutor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.impl.MergeQueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.TimestampGenerator;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.TimestampGeneratorByQueryFilterImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReaderByTimeStamp;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesReaderFromSingleFileByTimestampImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.impl.SeriesReaderFromSingleFileWithFilterImpl;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * IoTDB query executor with  global time filter
 * */
public class IoTDBQueryWithGlobalTimeFilterExecutorImpl implements IoTDBQueryExecutor {
    IoTDBTimeGenerator timestampGenerator;

    public IoTDBQueryWithGlobalTimeFilterExecutorImpl() {
    }

    @Override
    public QueryDataSet execute(QueryExpression queryExpression) throws IOException, FileNodeManagerException {

        this.timestampGenerator = new IoTDBTimeGenerator(queryExpression.getQueryFilter());

        LinkedHashMap<Path, SeriesReader> readersOfSelectedSeries = new LinkedHashMap<>();
        Filter<Long> timeFilter = ((GlobalTimeFilter) queryExpression.getQueryFilter()).getFilter();
        initReadersOfSelectedSeries(readersOfSelectedSeries, queryExpression.getSelectedSeries(), timeFilter);
        return new MergeQueryDataSet(readersOfSelectedSeries);
    }

    private void initReadersOfSelectedSeries(LinkedHashMap<Path, SeriesReader> readersOfSelectedSeries,
                                             List<Path> selectedSeries, Filter<Long> timeFilter) throws IOException, FileNodeManagerException {
        for (Path path : selectedSeries) {
            SeriesFilter<Long> seriesFilter = new SeriesFilter<Long>(path, timeFilter);
            QueryDataSource queryDataSource = IoTDBQueryDataSourceExecutor.getQueryDataSource(seriesFilter);
            SeriesReader seriesReader = new IoTDBQueryWithFilterReader(queryDataSource, seriesFilter);
            readersOfSelectedSeries.put(path, seriesReader);
        }
    }

}
