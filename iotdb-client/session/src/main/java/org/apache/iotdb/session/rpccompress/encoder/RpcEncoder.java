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
package org.apache.iotdb.session.rpccompress.encoder;

import org.apache.iotdb.session.rpccompress.EncodingTypeNotSupportedException;
import org.apache.iotdb.session.rpccompress.MetaHead;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.PublicBAOS;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Map;

public class RpcEncoder {
  /** MetaHead for values */
  private final MetaHead metaHeadForValues;

  /** MetaHead for timestamps */
  private final MetaHead metaHeadForTimeStamp;

  /**
   * User-specified encoding configurations for time series columns. Can be modified to apply
   * different encoding strategies per data type.
   */
  private final Map<TSDataType, TSEncoding> columnEncodersMap;

  /** Store the length of the MetaHead using a 4-byte integer. */
  private final int metaHeadLengthInBytes = 4;

  public RpcEncoder(Map<TSDataType, TSEncoding> columnEncodersMap) {
    this.columnEncodersMap = columnEncodersMap;
    this.metaHeadForValues = new MetaHead();
    this.metaHeadForTimeStamp = new MetaHead();
  }

  /**
   * Get the timestamp column in tablet
   *
   * @param tablet data
   * @throws IOException An IO exception occurs
   */
  public ByteArrayOutputStream encodeTimestamps(Tablet tablet) {
    // 1.encoder
    PublicBAOS publicBAOS = new PublicBAOS();
    ColumnEncoder encoder =
        createEncoder(
            TSDataType.INT64, columnEncodersMap.getOrDefault(TSDataType.INT64, TSEncoding.PLAIN));
    try {
      long[] timestamps = tablet.getTimestamps();
      encoder.encode(timestamps, publicBAOS);

      // 2.Get ColumnEntry content and add to metaHead
      metaHeadForTimeStamp.addColumnEntry(encoder.getColumnEntry());

      // 3.Serialize metaHead and append it to the output stream
      byte[] metaHeadEncoder = getMetaHeadForTimeStamp().toBytes();

      publicBAOS.write(metaHeadEncoder);

      // 4.Write the length of the serialized metaHead as a 4-byte big-endian integer
      publicBAOS.write(intToBytes(metaHeadEncoder.length));

    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return publicBAOS;
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
      case GORILLA:
        return new GorillaColumnEncoder(dataType);
      case ZIGZAG:
        return new ZigzagColumnEncoder(dataType);
      case CHIMP:
        return new ChimpColumnEncoder(dataType);
      case SPRINTZ:
        return new SprintzColumnEncoder(dataType);
      case RLBE:
        return new RlbeColumnEncoder(dataType);
      case DICTIONARY:
        return new DictionaryColumnEncoder(dataType);
      default:
        throw new EncodingTypeNotSupportedException(encodingType.name());
    }
  }

  /** Get the value columns from the tablet and encode them into encodedValues */
  public ByteArrayOutputStream encodeValues(Tablet tablet) {
    // 1.Encode each column
    PublicBAOS out = new PublicBAOS();
    for (int i = 0; i < tablet.getSchemas().size(); i++) {
      IMeasurementSchema schema = tablet.getSchemas().get(i);
      encodeColumn(schema.getType(), tablet, i, out);
    }

    try {
      // 2.Serializing MetaHead and  append it to the output stream
      byte[] metaHeadEncoder = getMetaHead().toBytes();
      out.write(metaHeadEncoder);
      // 3.Write the length of the serialized metaHead as a 4-byte big-endian integer
      out.write(intToBytes(metaHeadEncoder.length));
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return out;
  }

  public void encodeColumn(
      TSDataType dataType, Tablet tablet, int columnIndex, ByteArrayOutputStream out) {
    // 1.Get the encoding type
    TSEncoding encoding = columnEncodersMap.getOrDefault(dataType, TSEncoding.PLAIN);
    ColumnEncoder encoder = createEncoder(dataType, encoding);

    // 2.Get the data of the column
    Object columnValues = tablet.getValues()[columnIndex];

    // 3.encode
    switch (dataType) {
      case INT32:
        encoder.encode((int[]) columnValues, out);
        break;
      case INT64:
        encoder.encode((long[]) columnValues, out);
        break;
      case FLOAT:
        encoder.encode((float[]) columnValues, out);
        break;
      case DOUBLE:
        encoder.encode((double[]) columnValues, out);
        break;
      case BOOLEAN:
        encoder.encode((boolean[]) columnValues, out);
        break;
      case STRING:
      case BLOB:
      case TEXT:
        encoder.encode((Binary[]) columnValues, out);
        break;
      default:
        throw new UnsupportedOperationException("Unsupported data types: " + dataType);
    }

    // 4.Get ColumnEntry content and add to metaHead
    metaHeadForValues.addColumnEntry(encoder.getColumnEntry());
  }

  public static byte[] intToBytes(int value) {
    return new byte[] {
      (byte) ((value >>> 24) & 0xFF),
      (byte) ((value >>> 16) & 0xFF),
      (byte) ((value >>> 8) & 0xFF),
      (byte) (value & 0xFF)
    };
  }

  /**
   * Get metadata header
   *
   * @return metaHeader
   */
  public MetaHead getMetaHead() {
    return metaHeadForValues;
  }

  public MetaHead getMetaHeadForTimeStamp() {
    return metaHeadForTimeStamp;
  }
}
