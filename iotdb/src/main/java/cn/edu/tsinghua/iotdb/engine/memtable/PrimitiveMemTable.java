package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;

/**
 * Created by zhangjinrui on 2018/1/25.
 */
public class PrimitiveMemTable extends AbstractMemTable {
    @Override
    protected IMemSeries genMemSeries(TSDataType dataType) {
        return new PrimitiveMemSeries(dataType);
    }
}
