package cn.edu.tsinghua.tsfile.timeseries.readV2.basis;

import cn.edu.tsinghua.tsfile.common.utils.ITsRandomAccessFileReader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerier;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.MetadataQuerierByFileImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoader;
import cn.edu.tsinghua.tsfile.timeseries.readV2.controller.SeriesChunkLoaderImpl;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryDataSet;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.QueryExpression;
import cn.edu.tsinghua.tsfile.timeseries.readV2.query.impl.QueryExecutorRouter;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2017/12/27.
 */
public class ReadOnlyTsFile {

    private ITsRandomAccessFileReader randomAccessFileReader;
    private MetadataQuerier metadataQuerier;
    private SeriesChunkLoader seriesChunkLoader;
    private QueryExecutorRouter queryExecutorRouter;

    public ReadOnlyTsFile(ITsRandomAccessFileReader randomAccessFileReader) throws IOException {
        this.randomAccessFileReader = randomAccessFileReader;
        this.metadataQuerier = new MetadataQuerierByFileImpl(randomAccessFileReader);
        this.seriesChunkLoader = new SeriesChunkLoaderImpl(randomAccessFileReader);
        queryExecutorRouter = new QueryExecutorRouter(metadataQuerier, seriesChunkLoader);
    }

    public QueryDataSet query(QueryExpression queryExpression) throws IOException {
        return queryExecutorRouter.execute(queryExpression);
    }

    public void close() throws IOException {
        randomAccessFileReader.close();
    }
}
