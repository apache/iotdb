package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AlignedWritableMemChunk implements IWritableMemChunk {

  private IMeasurementSchema schema;
  private AlignedTVList list;
  private Map<String, Integer> alignedMeasurementIndexMap;
  private static final String UNSUPPORTED_TYPE = "Unsupported data type:";
  private static final Logger LOGGER = LoggerFactory.getLogger(AlignedWritableMemChunk.class);

  public AlignedWritableMemChunk(VectorMeasurementSchema schema) {
    this.schema = schema;
    alignedMeasurementIndexMap = new HashMap<>();
    for (int i = 0; i < schema.getSubMeasurementsCount(); i++) {
      alignedMeasurementIndexMap.put(schema.getSubMeasurementsList().get(i), i);
    }
    this.list = TVListAllocator.getInstance().allocate(schema.getSubMeasurementsTSDataTypeList());
  }

  public boolean containsMeasurement(String measurementId) {
    return alignedMeasurementIndexMap.containsKey(measurementId);
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
  public void putAlignedValue(long t, Object[] v, int[] columnOrder) {
    list.putAlignedValue(t, v, columnOrder);
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
  public void putAlignedValues(
      long[] t, Object[] v, BitMap[] bitMaps, int[] columnOrder, int start, int end) {
    list.putAlignedValues(t, v, bitMaps, columnOrder, start, end);
  }

  @Override
  public void write(long insertTime, Object objectValue) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public void writeAlignedValue(long insertTime, Object[] objectValue, IMeasurementSchema schema) {
    int[] columnOrder = checkColumnOrder(schema);
    putAlignedValue(insertTime, objectValue, columnOrder);
  }

  @Override
  public void write(
      long[] times, Object valueList, BitMap bitMap, TSDataType dataType, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public void writeVector(
      long[] times,
      Object[] valueList,
      BitMap[] bitMaps,
      IMeasurementSchema schema,
      int start,
      int end) {
    int[] columnOrder = checkColumnOrder(schema);
    putAlignedValues(times, valueList, bitMaps, columnOrder, start, end);
  }

  private int[] checkColumnOrder(IMeasurementSchema schema) {
    VectorMeasurementSchema vectorSchema = (VectorMeasurementSchema) schema;
    List<String> measurementIdList = vectorSchema.getSubMeasurementsList();
    int[] columnOrder = new int[measurementIdList.size()];
    for (int i = 0; i < measurementIdList.size(); i++) {
      columnOrder[i] = alignedMeasurementIndexMap.get(measurementIdList.get(i));
    }
    return columnOrder;
  }

  @Override
  public TVList getTVList() {
    return list;
  }

  @Override
  public long count() {
    return list.size() * alignedMeasurementIndexMap.size();
  }

  @Override
  public IMeasurementSchema getSchema() {
    return schema;
  }

  @Override
  public TVList getSortedTvListForQuery() {
    sortTVList();
    // increase reference count
    list.increaseReferenceCount();
    return list;
  }

  @Override
  public TVList getSortedTvListForQuery(List<IMeasurementSchema> schemaList) {
    sortTVList();
    // increase reference count
    list.increaseReferenceCount();
    List<Integer> columnIndexList = new ArrayList<>();
    List<TSDataType> dataTypeList = new ArrayList<>();
    for (IMeasurementSchema measurementSchema : schemaList) {
      columnIndexList.add(
          alignedMeasurementIndexMap.getOrDefault(measurementSchema.getMeasurementId(), -1));
      dataTypeList.add(measurementSchema.getType());
    }
    return list.getTvListByColumnIndex(columnIndexList, dataTypeList);
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
    return list.delete(lowerBound, upperBound);
  }

  @Override
  // TODO: THIS METHOLD IS FOR DELETING ONE COLUMN OF A VECTOR
  public int delete(long lowerBound, long upperBound, String measurementId) {
    // TODO Auto-generated method stub
    return 0;
  }

  @Override
  public IChunkWriter createIChunkWriter() {
    return new AlignedChunkWriterImpl(schema);
  }

  @Override
  public void encode(IChunkWriter chunkWriter) {

    List<Integer> timeDuplicateAlignedRowIndexList = null;
    for (int sortedRowIndex = 0; sortedRowIndex < list.size(); sortedRowIndex++) {
      long time = list.getTime(sortedRowIndex);

      // skip duplicated data
      if ((sortedRowIndex + 1 < list.size() && (time == list.getTime(sortedRowIndex + 1)))) {
        // record the time duplicated row index list for vector type
        if (timeDuplicateAlignedRowIndexList == null) {
          timeDuplicateAlignedRowIndexList = new ArrayList<>();
          timeDuplicateAlignedRowIndexList.add(list.getValueIndex(sortedRowIndex));
        }
        timeDuplicateAlignedRowIndexList.add(list.getValueIndex(sortedRowIndex + 1));
        continue;
      }
      List<TSDataType> dataTypes = list.getTsDataTypes();
      int originRowIndex = list.getValueIndex(sortedRowIndex);
      for (int columnIndex = 0; columnIndex < dataTypes.size(); columnIndex++) {
        // write the time duplicated rows
        if (timeDuplicateAlignedRowIndexList != null
            && !timeDuplicateAlignedRowIndexList.isEmpty()) {
          originRowIndex =
              list.getValidRowIndexForTimeDuplicatedRows(
                  timeDuplicateAlignedRowIndexList, columnIndex);
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
                "AlignedWritableMemChunk does not support data type: {}",
                dataTypes.get(columnIndex));
            break;
        }
      }
      chunkWriter.write(time);
      timeDuplicateAlignedRowIndexList = null;
    }
  }
}
