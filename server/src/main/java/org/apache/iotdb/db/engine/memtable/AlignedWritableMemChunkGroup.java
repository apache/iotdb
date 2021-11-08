package org.apache.iotdb.db.engine.memtable;

import java.util.List;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

public class AlignedWritableMemChunkGroup implements IWritableMemChunkGroup {

  public AlignedWritableMemChunkGroup(List<IMeasurementSchema> schemaList) {
    // TODO Auto-generated constructor stub
  }

  @Override
  public void writeAlignedValues(long[] times, Object[] columns, BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList, int start, int end) {
    // TODO Auto-generated method stub

  }

  @Override
  public void writeValues(long[] times, Object[] columns, BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList, int start, int end) {
    // TODO Auto-generated method stub

  }

  @Override
  public void release() {
    // TODO Auto-generated method stub

  }

  @Override
  public long count() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public boolean contains(String measurement) {
    // TODO Auto-generated method stub
    return false;
  }

  @Override
  public void write(long insertTime, Object[] objectValue) {
    // TODO Auto-generated method stub

  }

}
