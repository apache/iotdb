package cn.edu.tsinghua.tsfile.timeseries.readV2.common;

import java.io.InputStream;

/**
 * Created by zhangjinrui on 2018/1/14.
 */
public interface SeriesChunk {

    SeriesChunkDescriptor getEncodedSeriesChunkDescriptor();

    InputStream getSeriesChunkBodyStream();
}
