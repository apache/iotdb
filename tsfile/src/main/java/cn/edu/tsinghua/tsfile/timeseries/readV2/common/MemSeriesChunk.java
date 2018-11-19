package cn.edu.tsinghua.tsfile.timeseries.readV2.common;

import java.io.ByteArrayInputStream;

/**
 * Created by zhangjinrui on 2017/12/25.
 */
public class MemSeriesChunk implements SeriesChunk{
    private EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor;
    private ByteArrayInputStream seriesChunkBodyStream;

    public MemSeriesChunk(EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor, ByteArrayInputStream seriesChunkBodyStream) {
        this.encodedSeriesChunkDescriptor = encodedSeriesChunkDescriptor;
        this.seriesChunkBodyStream = seriesChunkBodyStream;
    }

    public EncodedSeriesChunkDescriptor getEncodedSeriesChunkDescriptor() {
        return encodedSeriesChunkDescriptor;
    }

    public ByteArrayInputStream getSeriesChunkBodyStream() {
        return seriesChunkBodyStream;
    }
}
