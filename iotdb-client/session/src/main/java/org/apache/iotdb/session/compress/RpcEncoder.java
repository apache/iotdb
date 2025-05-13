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
package org.apache.iotdb.session.compress;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class RpcEncoder {
  private final MetaHead metaHead;
  private final MetaHead metaHeadForTimeStamp;
  private final Map<TSDataType, TSEncoding> columnEncodersMap;
  private final CompressionType compressionType;

  public RpcEncoder(
      Map<TSDataType, TSEncoding> columnEncodersMap, CompressionType compressionType) {
    this.columnEncodersMap = columnEncodersMap;
    this.compressionType = compressionType;
    this.metaHead = new MetaHead();
    this.metaHeadForTimeStamp = new MetaHead();
  }

  /**
   * Get the timestamp column in tablet
   *
   * @param tablet data
   * @throws IOException An IO exception occurs
   */
  public ByteBuffer encodeTimestamps(Tablet tablet) {

    // 1.Get timestamp data
    long[] timestamps = tablet.getTimestamps();
    List<Long> timestampsList = new ArrayList<>();
    // 2.transform List
    for (int i = 0; i < timestamps.length; i++) {
      timestampsList.add(timestamps[i]);
    }
    // 3.encoder
    byte[] encoded = encodeTimeStampColumn(TSDataType.INT64, timestampsList);
    // 4. Serializing MetaHead
    byte[] metaHeadEncoder = getMetaHeadForTimeStamp().toBytes();

    ByteBuffer timeBuffer = ByteBuffer.allocate(encoded.length + metaHeadEncoder.length + 4);
    timeBuffer.put(encoded);
    timeBuffer.put(metaHeadEncoder);
    // 5. metaHead size
    timeBuffer.putInt(metaHeadEncoder.length);
    System.out.println("metaHead size: " + metaHeadEncoder.length);
    // 6.Adjust the ByteBuffer limit to the actual length of the data written
    timeBuffer.flip();
    return timeBuffer;
  }

  /** Get the value columns from the tablet and encode them into encodedValues */
  public ByteBuffer encodeValues(Tablet tablet) {
    // 1. Estimated maximum space (assuming each column is at most rowSize * 16 bytes)
    int estimatedSize = tablet.getRowSize() * 16 * tablet.getSchemas().size();
    ByteBuffer valueBuffer = ByteBuffer.allocate(estimatedSize);

    // 2. Encode each column
    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      IMeasurementSchema schema = tablet.getSchemas().get(i);
      byte[] encoded = encodeColumn(schema.getType(), tablet, i);
      valueBuffer.put(encoded);
    }

    // 3. Serializing MetaHead
    byte[] metaHeadEncoder = getMetaHead().toBytes();
    valueBuffer.put(metaHeadEncoder);
    // 4. metaHead size
    valueBuffer.putInt(metaHeadEncoder.length);
    // 5. Adjust the ByteBuffer limit to the actual length of the data written
    valueBuffer.flip();
    return valueBuffer;
  }

  public byte[] encodeColumn(TSDataType dataType, Tablet tablet, int columnIndex) {
    // 1.Get the encoding type
    TSEncoding encoding = columnEncodersMap.getOrDefault(dataType, TSEncoding.PLAIN);
    ColumnEncoder encoder = createEncoder(dataType, encoding);
    // 2.Get the data of the column and convert it into a Lis
    Object columnValues = tablet.getValues()[columnIndex];
    List<?> valueList;
    switch (dataType) {
      case INT32:
        int[] intArray = (int[]) columnValues;
        List<Integer> intList = new ArrayList<>(intArray.length);
        for (int v : intArray) intList.add(v);
        valueList = intList;
        break;
      case INT64:
        long[] longArray = (long[]) columnValues;
        List<Long> longList = new ArrayList<>(longArray.length);
        for (long v : longArray) longList.add(v);
        valueList = longList;
        break;
      case FLOAT:
        float[] floatArray = (float[]) columnValues;
        List<Float> floatList = new ArrayList<>(floatArray.length);
        for (float v : floatArray) floatList.add(v);
        valueList = floatList;
        break;
      case DOUBLE:
        double[] doubleArray = (double[]) columnValues;
        List<Double> doubleList = new ArrayList<>(doubleArray.length);
        for (double v : doubleArray) doubleList.add(v);
        valueList = doubleList;
        break;
      case BOOLEAN:
        boolean[] boolArray = (boolean[]) columnValues;
        List<Boolean> boolList = new ArrayList<>(boolArray.length);
        for (boolean v : boolArray) boolList.add(v);
        valueList = boolList;
        break;
      case STRING:
      case TEXT:
        Object[] textArray = (Object[]) columnValues;
        List<Object> textList = new ArrayList<>(textArray.length);
        for (Object v : textArray) textList.add(v);
        valueList = textList;
        break;
      default:
        throw new UnsupportedOperationException("Unsupported data types: " + dataType);
    }

    // 3.encoder
    byte[] encoded = encoder.encode(valueList);
    // 4.Get ColumnEntry content and add to metaHead
    metaHead.addColumnEntry(encoder.getColumnEntry());
    // 5.Returns the encoded data
    return encoded;
  }

  public byte[] encodeTimeStampColumn(TSDataType dataType, List<Long> timestamps) {
    // 1.Get the encoding type
    TSEncoding encoding = columnEncodersMap.getOrDefault(dataType, TSEncoding.PLAIN);
    ColumnEncoder encoder = createEncoder(dataType, encoding);
    // 2.encoder
    byte[] encoded = encoder.encode(timestamps);
    // 3.Get ColumnEntry content and add to metaHead
    metaHeadForTimeStamp.addColumnEntry(encoder.getColumnEntry());
    // 4.Returns the encoded data
    return encoded;
  }

  /**
   * Create the corresponding encoder based on the data type and encoding type
   *
   * @param dataType data type
   * @param encodingType encoding Type
   * @return columnEncoder
   */
  private ColumnEncoder createEncoder(TSDataType dataType, TSEncoding encodingType) {
    switch (encodingType) {
      case PLAIN:
        return new PlainColumnEncoder(dataType);
      case RLE:
        return new RleColumnEncoder(dataType);
      case TS_2DIFF:
        return new Ts2DiffColumnEncoder(dataType);
      default:
        throw new EncodingTypeNotSupportedException(encodingType.name());
    }
  }

  /**
   * Get metadata header
   *
   * @return metaHeader
   */
  public MetaHead getMetaHead() {
    return metaHead;
  }

  public MetaHead getMetaHeadForTimeStamp() {
    return metaHeadForTimeStamp;
  }
}
