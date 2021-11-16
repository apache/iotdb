package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import java.util.List;
import java.util.Map;

public interface IWritableMemChunkGroup {

  void writeValues(
      long[] times,
      Object[] columns,
      BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList,
      int start,
      int end);

  void release();

  long count();

  boolean contains(String measurement);

  void write(long insertTime, Object[] objectValue, List<IMeasurementSchema> schemaList);

  Map<String, IWritableMemChunk> getMemChunkMap();

  long getCurrentChunkPointNum(String measurement);
}
