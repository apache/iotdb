package cn.edu.tsinghua.tsfile.timeseries.readV2.query;

import cn.edu.tsinghua.tsfile.timeseries.readV2.datatype.RowRecord;

import java.io.IOException;

/**
 * Created by zhangjinrui on 2017/12/13.
 */
public interface QueryDataSet {

    boolean hasNext() throws IOException;

    RowRecord next() throws IOException;

}
