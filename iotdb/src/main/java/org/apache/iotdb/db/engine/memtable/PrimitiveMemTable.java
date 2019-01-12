package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;


public class PrimitiveMemTable extends AbstractMemTable {
    @Override
    protected IWritableMemChunk genMemSeries(TSDataType dataType) {
        return new WritableMemChunk(dataType);
    }
}
