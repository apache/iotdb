package cn.edu.tsinghua.iotdb.read.timegenerator;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.exception.FileNodeManagerException;
import cn.edu.tsinghua.iotdb.read.IoTDBQueryDataSourceExecutor;
import cn.edu.tsinghua.iotdb.read.reader.IoTDBQueryWithFilterReader;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.expression.impl.SeriesFilter;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.timegenerator.NodeConstructor;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;

import java.io.IOException;

public class IoTDBNodeConstructor extends NodeConstructor {

    @Override
    public SeriesReader generateSeriesReader(SeriesFilter<?> seriesFilter) throws IOException {
        QueryDataSource queryDataSource = null;
        try {
            queryDataSource = IoTDBQueryDataSourceExecutor.getQueryDataSource(seriesFilter);
        } catch (FileNodeManagerException e) {
            e.printStackTrace();
        }
        return new IoTDBQueryWithFilterReader(queryDataSource, seriesFilter);
    }


}
