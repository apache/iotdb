package cn.edu.tsinghua.iotdb.queryV2.engine.reader.component;

import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunkDescriptor;

import java.io.InputStream;

/**
 * Created by zhangjinrui on 2018/1/18.
 */
public class BufferedSeriesChunk implements SeriesChunk {

    private SegmentInputStream seriesChunkInputStream;
    private SeriesChunkDescriptor seriesChunkDescriptor;

    public BufferedSeriesChunk(SegmentInputStream seriesChunkInputStream, SeriesChunkDescriptor seriesChunkDescriptor) {
        this.seriesChunkInputStream = seriesChunkInputStream;
        this.seriesChunkDescriptor = seriesChunkDescriptor;
    }

    @Override
    public SeriesChunkDescriptor getEncodedSeriesChunkDescriptor() {
        return this.seriesChunkDescriptor;
    }

    @Override
    public InputStream getSeriesChunkBodyStream() {
        return this.seriesChunkInputStream;
    }

}
