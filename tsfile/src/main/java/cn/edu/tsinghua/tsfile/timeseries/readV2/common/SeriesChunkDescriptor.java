package cn.edu.tsinghua.tsfile.timeseries.readV2.common;

import cn.edu.tsinghua.tsfile.file.metadata.TsDigest;
import cn.edu.tsinghua.tsfile.file.metadata.enums.CompressionTypeName;
import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

/**
 * Created by zhangjinrui on 2018/1/15.
 */
public interface SeriesChunkDescriptor {
    TSDataType getDataType();

    TsDigest getValueDigest();

    long getMinTimestamp();

    long getMaxTimestamp();

    long getCountOfPoints();

    CompressionTypeName getCompressionTypeName();
}
