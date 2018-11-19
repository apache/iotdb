package cn.edu.tsinghua.iotdb.queryV2.engine.reader.util;

import cn.edu.tsinghua.tsfile.timeseries.readV2.common.SeriesChunkDescriptor;

import java.util.List;

/**
 * Group all SeriesDescriptor by respective timestamp interval. The SeriesDescriptors whose timestamp intervals are overlapped will
 * be divided into one group.
 * Created by zhangjinrui on 2018/1/15.
 */
public interface SeriesChunkGrouper {
    List<List<SeriesChunkDescriptor>> group(List<SeriesChunkDescriptor> seriesChunkDescriptorList);
}
