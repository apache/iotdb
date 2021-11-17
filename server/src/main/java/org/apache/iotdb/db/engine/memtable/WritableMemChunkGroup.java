package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class WritableMemChunkGroup implements IWritableMemChunkGroup {

  private Map<String, IWritableMemChunk> memChunkMap;

  public WritableMemChunkGroup(List<IMeasurementSchema> schemaList) {
    memChunkMap = new HashMap<>();
    for (IMeasurementSchema schema : schemaList) {
      createMemChunkIfNotExistAndGet(schema);
    }
  }

  @Override
  public void writeValues(
      long[] times,
      Object[] columns,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end) {
    int emptyColumnCount = 0;
    for (int i = 0; i < columns.length; i++) {
      if (columns[i] == null) {
        emptyColumnCount++;
        continue;
      }
      IWritableMemChunk memChunk =
          createMemChunkIfNotExistAndGet(schemaList.get(i - emptyColumnCount));
      memChunk.write(
          times,
          columns[i],
          bitMaps == null ? null : bitMaps[i],
          schemaList.get(i - emptyColumnCount).getType(),
          start,
          end);
    }
  }

  private IWritableMemChunk createMemChunkIfNotExistAndGet(IMeasurementSchema schema) {
    return memChunkMap.computeIfAbsent(
        schema.getMeasurementId(),
        k -> {
          return new WritableMemChunk(schema);
        });
  }

  @Override
  public void release() {
    for (IWritableMemChunk memChunk : memChunkMap.values()) {
      memChunk.release();
    }
  }

  @Override
  public long count() {
    long count = 0;
    for (IWritableMemChunk memChunk : memChunkMap.values()) {
      count += memChunk.count();
    }
    return count;
  }

  @Override
  public boolean contains(String measurement) {
    return memChunkMap.containsKey(measurement);
  }

  @Override
  public void write(long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList) {
    int emptyColumnCount = 0;
    for (int i = 0; i < objectValue.length; i++) {
      if (objectValue[i] == null) {
        emptyColumnCount++;
        continue;
      }
      IWritableMemChunk memChunk =
          createMemChunkIfNotExistAndGet(schemaList.get(i - emptyColumnCount));
      memChunk.write(insertTime, objectValue[i]);
    }
  }

  @Override
  public Map<String, IWritableMemChunk> getMemChunkMap() {
    return memChunkMap;
  }

  @Override
  public long getCurrentChunkPointNum(String measurement) {
    return memChunkMap.get(measurement).count();
  }
}
