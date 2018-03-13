package cn.edu.tsinghua.iotdb.queryV2.engine.externalsort.serialize;

import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.TimeValuePair;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2018/1/20.
 */
public interface TimeValuePairSerializer {

    void write(TimeValuePair timeValuePair) throws IOException;

    void close() throws IOException;
}
