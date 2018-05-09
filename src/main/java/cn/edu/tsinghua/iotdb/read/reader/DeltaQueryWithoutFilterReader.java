package cn.edu.tsinghua.iotdb.read.reader;

import cn.edu.tsinghua.iotdb.engine.querycontext.QueryDataSource;
import cn.edu.tsinghua.iotdb.engine.querycontext.UpdateDeleteInfoOfOneSeries;
import cn.edu.tsinghua.iotdb.queryV2.engine.reader.series.OverflowInsertDataReader;
import cn.edu.tsinghua.iotdb.queryV2.factory.SeriesReaderFactory;
import cn.edu.tsinghua.tsfile.timeseries.read.support.Path;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.reader.SeriesReader;

import java.io.IOException;

public class DeltaQueryWithoutFilterReader implements SeriesReader{

    private Path seriesPath;
    private SeriesReader tsFilesReader;
    private OverflowInsertDataReader overflowInsertDataReader;
    private UpdateDeleteInfoOfOneSeries updateDeleteInfoOfOneSeries;

    public DeltaQueryWithoutFilterReader(Path seriesPath, QueryDataSource queryDataSource) throws IOException {
        this.seriesPath = seriesPath;
        this.tsFilesReader = new DeltaTsFilesReader(queryDataSource.getSeriesDataSource(),
                queryDataSource.getOverflowSeriesDataSource().getUpdateDeleteInfoOfOneSeries().getOverflowUpdateOperationReaderNewInstance());
        this.overflowInsertDataReader = SeriesReaderFactory.getInstance().
                createSeriesReaderForOverflowInsert(queryDataSource.getOverflowSeriesDataSource());
    }

    @Override
    public boolean hasNext() throws IOException {
        return false;
    }

    @Override
    public TimeValuePair next() throws IOException {
        return null;
    }

    @Override
    public void skipCurrentTimeValuePair() throws IOException {

    }

    @Override
    public void close() throws IOException {

    }
}
