package org.apache.iotdb.db.engine.memtable;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

public interface IWritableMemChunkGroup {

  void writeAlignedValues(long[] times, Object[] columns, BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList, int start, int end);

  void writeValues(long[] times, Object[] columns, BitMap[] bitMaps,
      List<IMeasurementSchema> schemaList, int start, int end);

  void release();

  long count();

  boolean contains(String measurement);

  void write(long insertTime, Object[] objectValue);

  Map<String, IWritableMemChunk> getMemChunkMap();

}
