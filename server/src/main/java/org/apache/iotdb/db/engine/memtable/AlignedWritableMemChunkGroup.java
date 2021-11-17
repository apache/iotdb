package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AlignedWritableMemChunkGroup implements IWritableMemChunkGroup {

  private AlignedWritableMemChunk memChunk;

  public AlignedWritableMemChunkGroup(List<IMeasurementSchema> schemaList) {
    memChunk = new AlignedWritableMemChunk(schemaList);
  }

  @Override
  public void writeValues(
      long[] times,
      Object[] columns,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end) {
    memChunk.writeAlignedValues(times, columns, bitMaps, schemaList, start, end);
  }

  @Override
  public void release() {
    memChunk.release();
  }

  @Override
  public long count() {
    return memChunk.count();
  }

  @Override
  public boolean contains(String measurement) {
    return memChunk.containsMeasurement(measurement);
  }

  @Override
  public void write(long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList) {
    memChunk.writeAlignedValue(insertTime, objectValue, schemaList);
  }

  @Override
  public Map<String, IWritableMemChunk> getMemChunkMap() {
    return Collections.singletonMap("", memChunk);
  }

  @Override
  public long getCurrentChunkPointNum(String measurement) {
    return memChunk.count();
  }

  public AlignedWritableMemChunk getAlignedMemChunk() {
    return memChunk;
  }
}
