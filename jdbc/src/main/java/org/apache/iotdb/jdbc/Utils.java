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
package org.apache.iotdb.jdbc;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Properties;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BytesUtils;

/**
 * Utils to convert between thrift format and TsFile format.
 */
public class Utils {

  /**
   * Parse JDBC connection URL The only supported format of the URL is:
   * jdbc:iotdb://localhost:6667/.
   */
  static IoTDBConnectionParams parseUrl(String url, Properties info)
      throws IoTDBURLException {
    IoTDBConnectionParams params = new IoTDBConnectionParams(url);
    if (url.trim().equalsIgnoreCase(Config.IOTDB_URL_PREFIX)) {
      return params;
    }
    boolean isUrlLegal = false;
    Pattern pattern = Pattern.compile("([^:]+):([0-9]{1,5})/?");
    String subURL = url.substring(Config.IOTDB_URL_PREFIX.length());
    Matcher matcher = pattern.matcher(subURL);
    if(matcher.matches()) {
      isUrlLegal = true;
    }
    if (!isUrlLegal) {
      throw new IoTDBURLException("Error url format, url should be jdbc:iotdb://anything:port/ or jdbc:iotdb://anything:port");
    }
    params.setHost(matcher.group(1));
    params.setPort(Integer.parseInt(matcher.group(2)));

    if (info.containsKey(Config.AUTH_USER)) {
      params.setUsername(info.getProperty(Config.AUTH_USER));
    }
    if (info.containsKey(Config.AUTH_PASSWORD)) {
      params.setPassword(info.getProperty(Config.AUTH_PASSWORD));
    }

    return params;
  }
  
  static ByteBuffer convertResultBuffer(TSQueryDataSet tsQueryDataSet,
      List<String> columnTypeList) throws IOException {
    int rowCount = tsQueryDataSet.getRowCount();
    ByteBuffer byteBuffer = tsQueryDataSet.bufferForValues();
    DataOutputStream[] dataOutputStreams = new DataOutputStream[rowCount];
    ByteArrayOutputStream[] byteArrayOutputStreams = new ByteArrayOutputStream[rowCount];

    // process time buffer
    processTimestamp(byteBuffer, rowCount, dataOutputStreams, byteArrayOutputStreams);
    int valueOccupation = 0;
    for (String type : columnTypeList) {
      TSDataType dataType = TSDataType.valueOf(type);
      for (int i = 0; i < rowCount; i++) {
        boolean is_empty = BytesUtils.byteToBool(byteBuffer.get());
        if (is_empty) {
          dataOutputStreams[i].writeBoolean(true);
        } else {
          dataOutputStreams[i].writeBoolean(false);
          switch (dataType) {
            case BOOLEAN:
              boolean booleanValue = BytesUtils.byteToBool(byteBuffer.get());
              dataOutputStreams[i].writeBoolean(booleanValue);
              valueOccupation += 1;
              break;
            case INT32:
              int intValue = byteBuffer.getInt();
              dataOutputStreams[i].writeInt(intValue);
              valueOccupation += 4;
              break;
            case INT64:
              long longValue = byteBuffer.getLong();
              dataOutputStreams[i].writeLong(longValue);
              valueOccupation += 8;
              break;
            case FLOAT:
              float floatValue = byteBuffer.getFloat();
              dataOutputStreams[i].writeFloat(floatValue);
              valueOccupation += 4;
              break;
            case DOUBLE:
              double doubleValue = byteBuffer.getDouble();
              dataOutputStreams[i].writeDouble(doubleValue);
              valueOccupation += 8;
              break;
            case TEXT:
              int binarySize = byteBuffer.getInt();
              byte[] binaryValue = new byte[binarySize];
              byteBuffer.get(binaryValue);
              dataOutputStreams[i].writeInt(binarySize);
              dataOutputStreams[i].write(binaryValue);
              valueOccupation = valueOccupation + 4 + binaryValue.length;
              break;
            default:
              throw new UnSupportedDataTypeException(
                  String.format("Data type %s is not supported.", type));
          }
        }
      }
    }
    valueOccupation += rowCount * 8;
    valueOccupation += rowCount * columnTypeList.size();
    ByteBuffer resultBuffer = ByteBuffer.allocate(valueOccupation);
    for (int i = 0; i < rowCount; i++) {
      resultBuffer.put(byteArrayOutputStreams[i].toByteArray());
    }
    resultBuffer.flip(); // PAY ATTENTION TO HERE
    
    return resultBuffer;
  }

  private static void processTimestamp(ByteBuffer byteBuffer, int rowCount, 
      DataOutputStream[] dataOutputStreams, ByteArrayOutputStream[] byteArrayOutputStreams) 
      throws IOException {
    for (int i = 0; i < rowCount; i++) {
      byteArrayOutputStreams[i] = new ByteArrayOutputStream();
      dataOutputStreams[i] = new DataOutputStream(byteArrayOutputStreams[i]);
      long timestamp = byteBuffer.getLong(); // byteBuffer has been flipped by the server side
      dataOutputStreams[i].writeLong(timestamp);
    }
  }
  
  public static RowRecord getRowRecord(ByteBuffer byteBuffer, List<String> columnTypeList) 
      throws BufferUnderflowException {
    if (byteBuffer.hasRemaining()) {
      long timestamp = byteBuffer.getLong();
      RowRecord record = new RowRecord(timestamp);
      Field field = null;
      for (String type : columnTypeList) {
        boolean is_empty = BytesUtils.byteToBool(byteBuffer.get());
        if (is_empty) {
          field = new Field(null);
        } else {
          TSDataType dataType = TSDataType.valueOf(type);
          field = new Field(dataType);
          switch (dataType) {
            case BOOLEAN:
              boolean booleanValue = BytesUtils.byteToBool(byteBuffer.get());
              field.setBoolV(booleanValue);
              break;
            case INT32:
              int intValue = byteBuffer.getInt();
              field.setIntV(intValue);
              break;
            case INT64:
              long longValue = byteBuffer.getLong();
              field.setLongV(longValue);
              break;
            case FLOAT:
              float floatValue = byteBuffer.getFloat();
              field.setFloatV(floatValue);
              break;
            case DOUBLE:
              double doubleValue = byteBuffer.getDouble();
              field.setDoubleV(doubleValue);
              break;
            case TEXT:
              int binarySize = byteBuffer.getInt();
              byte[] binaryValue = new byte[binarySize];
              byteBuffer.get(binaryValue);
              field.setBinaryV(new Binary(binaryValue));
              break;
            default:
              throw new UnSupportedDataTypeException(
                  String.format("Data type %s is not supported.", type));
          }
        }
        record.getFields().add(field);
      }
      return record;
    }
    else {
      return null;
    }
  }
  
}