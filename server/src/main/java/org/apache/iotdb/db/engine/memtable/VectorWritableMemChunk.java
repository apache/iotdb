package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.db.utils.datastructure.VectorTVList;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.chunk.VectorChunkWriterImpl;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class VectorWritableMemChunk implements IWritableMemChunk {

  private IMeasurementSchema schema;
  private VectorTVList list;
  private Map<String, Integer> VectorIdIndexMap;
  private static final String UNSUPPORTED_TYPE = "Unsupported data type:";
  private static final Logger LOGGER = LoggerFactory.getLogger(VectorWritableMemChunk.class);

  public VectorWritableMemChunk(IMeasurementSchema schema) {
    this.schema = schema;
    VectorIdIndexMap = new HashMap<>();
    for (int i = 0; i < schema.getSubMeasurementsCount(); i++) {
      VectorIdIndexMap.put(schema.getSubMeasurementsList().get(i), i);
    }
    this.list = TVListAllocator.getInstance().allocate(schema.getSubMeasurementsTSDataTypeList());
  }

  @Override
  public void putLong(long t, long v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public void putInt(long t, int v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public void putFloat(long t, float v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public void putDouble(long t, double v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public void putBinary(long t, Binary v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public void putBoolean(long t, boolean v) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public void putVector(long t, Object[] v, int[] columnOrder) {
    list.putVector(t, v, columnOrder);
  }

  @Override
  public void putLongs(long[] t, long[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public void putInts(long[] t, int[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public void putFloats(long[] t, float[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public void putDoubles(long[] t, double[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public void putBinaries(long[] t, Binary[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public void putBooleans(long[] t, boolean[] v, BitMap bitMap, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public void putVectors(long[] t, Object[] v, BitMap[] bitMaps, int[] columnOrder, int start, int end) {
    list.putVectors(t, v, bitMaps, columnOrder, start, end);
  }

  @Override
  public void write(long insertTime, Object objectValue) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public void writeVector(long insertTime, String[] measurementIds, Object[] objectValue) {
    int[] columnOrder = checkColumnOrder(measurementIds);
    putVector(insertTime, objectValue, columnOrder);
  }

  @Override
  public void write(
      long[] times, Object valueList, BitMap bitMap, TSDataType dataType, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public void writeVector(
      long[] times,
      String[] measurementIds,
      Object[] valueList,
      BitMap[] bitMaps,
      int start,
      int end) {
    int[] columnOrder = checkColumnOrder(measurementIds);
    putVectors(times, valueList, bitMaps, columnOrder, start, end);
  }

  private int[] checkColumnOrder(String[] measurementIds) {
    int[] columnOrder = new int[measurementIds.length];
    
    return columnOrder;
  }

  @Override
  public long count() {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public IMeasurementSchema getSchema() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TVList getSortedTvListForQuery() {
    // TODO Auto-generated method stub
    return null;
  }

  @Override
  public TVList getSortedTvListForQuery(List<Integer> columnIndexList) {
    sortTVList();
    // increase reference count
    list.increaseReferenceCount();
    return list.getTvListByColumnIndex(columnIndexList);
  }

  private void sortTVList() {
    // check reference count
    if ((list.getReferenceCount() > 0 && !list.isSorted())) {
      list = list.clone();
    }

    if (!list.isSorted()) {
      list.sort();
    }
  }

  @Override
  public void sortTvListForFlush() {
    sortTVList();
  }

  @Override
  public int delete(long lowerBound, long upperBound) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public int delete(long lowerBound, long upperBound, int columnIndex) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public IChunkWriter createIChunkWriter() {
    return new VectorChunkWriterImpl(schema);
  }

  @Override
  public void encode(IChunkWriter chunkWriter) {

    List<Integer> timeDuplicatedVectorRowIndexList = null;
    for (int sortedRowIndex = 0; sortedRowIndex < list.size(); sortedRowIndex++) {
      long time = list.getTime(sortedRowIndex);

      // skip duplicated data
      if ((sortedRowIndex + 1 < list.size() && (time == list.getTime(sortedRowIndex + 1)))) {
        // record the time duplicated row index list for vector type
        if (timeDuplicatedVectorRowIndexList == null) {
          timeDuplicatedVectorRowIndexList = new ArrayList<>();
          timeDuplicatedVectorRowIndexList.add(list.getValueIndex(sortedRowIndex));
        }
        timeDuplicatedVectorRowIndexList.add(list.getValueIndex(sortedRowIndex + 1));
        continue;
      }
      List<TSDataType> dataTypes = list.getTsDataTypes();
      int originRowIndex = list.getValueIndex(sortedRowIndex);
      for (int columnIndex = 0; columnIndex < dataTypes.size(); columnIndex++) {
        // write the time duplicated rows
        if (timeDuplicatedVectorRowIndexList != null
            && !timeDuplicatedVectorRowIndexList.isEmpty()) {
          originRowIndex =
              list.getValidRowIndexForTimeDuplicatedRows(
                  timeDuplicatedVectorRowIndexList, columnIndex);
        }
        boolean isNull = list.isValueMarked(originRowIndex, columnIndex);
        switch (dataTypes.get(columnIndex)) {
          case BOOLEAN:
            chunkWriter.write(
                time, list.getBooleanByValueIndex(originRowIndex, columnIndex), isNull);
            break;
          case INT32:
            chunkWriter.write(time, list.getIntByValueIndex(originRowIndex, columnIndex), isNull);
            break;
          case INT64:
            chunkWriter.write(time, list.getLongByValueIndex(originRowIndex, columnIndex), isNull);
            break;
          case FLOAT:
            chunkWriter.write(time, list.getFloatByValueIndex(originRowIndex, columnIndex), isNull);
            break;
          case DOUBLE:
            chunkWriter.write(
                time, list.getDoubleByValueIndex(originRowIndex, columnIndex), isNull);
            break;
          case TEXT:
            chunkWriter.write(
                time, list.getBinaryByValueIndex(originRowIndex, columnIndex), isNull);
            break;
          default:
            LOGGER.error(
                "VectorWritableMemChunk does not support data type: {}",
                dataTypes.get(columnIndex));
            break;
        }
      }
      chunkWriter.write(time);
      timeDuplicatedVectorRowIndexList = null;
    }
  }
}
