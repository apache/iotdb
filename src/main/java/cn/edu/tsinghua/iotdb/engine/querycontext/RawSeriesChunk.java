package cn.edu.tsinghua.iotdb.engine.querycontext;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;
import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TsPrimitiveType;

import java.util.Iterator;

/**
 * Created by zhangjinrui on 2018/1/21.
 */
public interface RawSeriesChunk {

    TSDataType getDataType();

    long getMaxTimestamp();

    long getMinTimestamp();

    TsPrimitiveType getMaxValue();

    TsPrimitiveType getMinValue();

    Iterator<TimeValuePair> getIterator();
    
    boolean isEmpty();
}
