package cn.edu.tsinghua.iotdb.read.executor;

import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.read.IoTDBQueryDataSourceExecutor;
import cn.edu.tsinghua.iotdb.read.reader.IoTDBQueryWithFilterReader;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.impl.MergeQueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;

/**
 * IoTDB query executor without filter
 * */
public class IoTDBQueryWithoutFilterExecutorImpl{

    public IoTDBQueryWithoutFilterExecutorImpl() {
    }

    public static QueryDataSet execute(QueryExpression queryExpression) throws IOException, FileNodeManagerException {
        LinkedHashMap<Path, SeriesReader> readersOfSelectedSeries = new LinkedHashMap<>();
        initReadersOfSelectedSeries(readersOfSelectedSeries, queryExpression.getSelectedSeries());
        return new MergeQueryDataSet(readersOfSelectedSeries);
    }

    private static void initReadersOfSelectedSeries(LinkedHashMap<Path, SeriesReader> readersOfSelectedSeries,
                                             List<Path> selectedSeries) throws IOException, FileNodeManagerException {
        for (Path path : selectedSeries) {
            QueryDataSource queryDataSource = IoTDBQueryDataSourceExecutor.getQueryDataSource(path);
            SeriesReader seriesReader = new IoTDBQueryWithFilterReader(queryDataSource);
            readersOfSelectedSeries.put(path, seriesReader);
        }
    }
}
