package cn.edu.tsinghua.tsfile.timeseries.readV2.controller;

import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunk;
import cn.edu.tsinghua.tsfile.timeseries.readV2.common.EncodedSeriesChunkDescriptor;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2017/12/26.
 */
public interface SeriesChunkLoader {
    SeriesChunk getMemSeriesChunk(EncodedSeriesChunkDescriptor encodedSeriesChunkDescriptor) throws IOException;
}
