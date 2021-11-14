/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.memtable;

import org.apache.iotdb.db.rescon.TVListAllocator;
import org.apache.iotdb.db.utils.datastructure.AlignedTVList;
import org.apache.iotdb.db.utils.datastructure.TVList;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BitMap;
import org.apache.iotdb.tsfile.write.chunk.AlignedChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.schema.VectorMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class AlignedWritableMemChunk implements IWritableMemChunk {

  private VectorMeasurementSchema schema;
  private AlignedTVList list;
  private static final String UNSUPPORTED_TYPE = "Unsupported data type:";
  private static final Logger LOGGER = LoggerFactory.getLogger(AlignedWritableMemChunk.class);

  public AlignedWritableMemChunk(VectorMeasurementSchema schema) {
    this.schema = schema;
    this.list = TVListAllocator.getInstance().allocate(schema.getSubMeasurementsTSDataTypeList());
  }

  public boolean containsMeasurement(String measurementId) {
    return schema.containsSubMeasurement(measurementId);
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
  public void putAlignedValue(long t, Object[] v, int[] columnIndexArray) {
    list.putAlignedValue(t, v, columnIndexArray);
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
      long[] t, Object[] v, BitMap[] bitMaps, int[] columnIndexArray, int start, int end) {
    list.putAlignedValues(t, v, bitMaps, columnIndexArray, start, end);
  }

  @Override
  public void write(long insertTime, Object objectValue) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public void writeAlignedValue(long insertTime, Object[] objectValue, IMeasurementSchema schema) {
    int[] columnIndexArray = checkColumnsInInsertPlan(schema);
    putAlignedValue(insertTime, objectValue, columnIndexArray);
  }

  @Override
  public void write(
      long[] times, Object valueList, BitMap bitMap, TSDataType dataType, int start, int end) {
    throw new UnSupportedDataTypeException(UNSUPPORTED_TYPE + schema.getType());
  }

  @Override
  public void writeAlignedValues(
      long[] times,
      Object[] valueList,
      BitMap[] bitMaps,
      IMeasurementSchema schema,
      int start,
      int end) {
    int[] columnIndexArray = checkColumnsInInsertPlan(schema);
    putAlignedValues(times, valueList, bitMaps, columnIndexArray, start, end);
  }

  private int[] checkColumnsInInsertPlan(IMeasurementSchema schema) {
    VectorMeasurementSchema vectorSchema = (VectorMeasurementSchema) schema;
    List<String> measurementIdsInInsertPlan = vectorSchema.getSubMeasurementsList();
    List<TSDataType> dataTypesInInsertPlan = vectorSchema.getSubMeasurementsTSDataTypeList();
    List<TSEncoding> encodingsInInsertPlan = vectorSchema.getSubMeasurementsTSEncodingList();
    for (int i = 0; i < measurementIdsInInsertPlan.size(); i++) {
      if (!containsMeasurement(measurementIdsInInsertPlan.get(i))) {
        this.schema.addSubMeasurement(
            measurementIdsInInsertPlan.get(i),
            dataTypesInInsertPlan.get(i),
            encodingsInInsertPlan.get(i));
        this.list.extendColumn(dataTypesInInsertPlan.get(i));
      }
    }
    List<String> measurementIdsInTVList =
        ((VectorMeasurementSchema) this.schema).getSubMeasurementsList();
    int[] columnIndexArray = new int[measurementIdsInTVList.size()];
    for (int i = 0; i < columnIndexArray.length; i++) {
      columnIndexArray[i] = measurementIdsInInsertPlan.indexOf(measurementIdsInTVList.get(i));
    }
    return columnIndexArray;
  }

  @Override
  public TVList getTVList() {
    return list;
  }

  @Override
  public long count() {
    return list.size() * schema.getSubMeasurementsCount();
  }

  public long alignedListSize() {
    return list.size();
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
    for (IMeasurementSchema measurementSchema : schemaList) {
      columnIndexList.add(schema.getSubMeasurementIndex(measurementSchema.getMeasurementId()));
    }
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
    return list.delete(lowerBound, upperBound);
  }

  @Override
  // TODO: THIS METHOLD IS FOR DELETING ONE COLUMN OF A VECTOR
  public int delete(long lowerBound, long upperBound, String measurementId) {
    return 0;
  }

  @Override
  public IChunkWriter createIChunkWriter() {
    return new AlignedChunkWriterImpl(schema);
  }

  @Override
  public void encode(IChunkWriter chunkWriter) {
    AlignedChunkWriterImpl alignedChunkWriter = (AlignedChunkWriterImpl) chunkWriter;
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
            alignedChunkWriter.write(
                time, list.getBooleanByValueIndex(originRowIndex, columnIndex), isNull);
            break;
          case INT32:
            alignedChunkWriter.write(
                time, list.getIntByValueIndex(originRowIndex, columnIndex), isNull);
            break;
          case INT64:
            alignedChunkWriter.write(
                time, list.getLongByValueIndex(originRowIndex, columnIndex), isNull);
            break;
          case FLOAT:
            alignedChunkWriter.write(
                time, list.getFloatByValueIndex(originRowIndex, columnIndex), isNull);
            break;
          case DOUBLE:
            alignedChunkWriter.write(
                time, list.getDoubleByValueIndex(originRowIndex, columnIndex), isNull);
            break;
          case TEXT:
            alignedChunkWriter.write(
                time, list.getBinaryByValueIndex(originRowIndex, columnIndex), isNull);
            break;
          default:
            LOGGER.error(
                "AlignedWritableMemChunk does not support data type: {}",
                dataTypes.get(columnIndex));
            break;
        }
      }
      alignedChunkWriter.write(time);
      timeDuplicateAlignedRowIndexList = null;
    }
  }
}
