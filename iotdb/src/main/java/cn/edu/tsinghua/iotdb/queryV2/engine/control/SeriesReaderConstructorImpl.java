package cn.edu.tsinghua.iotdb.queryV2.engine.control;

import cn.edu.tsinghua.iotdb.engine.filenode.FileNodeManager;
import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.tsfile.timeseries.filterV2.basic.Filter;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;

/**
 * Created by zhangjinrui on 2018/1/23.
 */
public class SeriesReaderConstructorImpl implements SeriesReaderConstructor {

    private FileNodeManager fileNodeManager;


    private SeriesReaderConstructorImpl() {
        fileNodeManager = FileNodeManager.getInstance();
    }

    @Override
    public SeriesReader create(QueryDataSource queryDataSource) {
        return null;
    }

    @Override
    public SeriesReader create(QueryDataSource queryDataSource, Filter<?> filter) {
        return null;
    }

    @Override
    public SeriesReader createSeriesReaderByTimestamp(QueryDataSource queryDataSource) {
        return null;
    }

    private static class SeriesReaderConstructorHelper {
        private static SeriesReaderConstructorImpl INSTANCE = new SeriesReaderConstructorImpl();
    }

    public SeriesReaderConstructorImpl getInstance() {
        return SeriesReaderConstructorHelper.INSTANCE;
    }
}
