package cn.edu.tsinghua.iotdb.engine.memtable;

import cn.edu.tsinghua.tsfile.file.metadata.enums.TSDataType;


public class PrimitiveMemTable extends AbstractMemTable {
    @Override
    protected IWritableMemChunk genMemSeries(TSDataType dataType) {
        return new WritableMemChunk(dataType);
    }
}
