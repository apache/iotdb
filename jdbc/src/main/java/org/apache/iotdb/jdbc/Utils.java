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

import java.nio.ByteBuffer;
import java.util.ArrayList;
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

    Pattern pattern = Pattern.compile("([^;]*):([^;]*)/");
    Matcher matcher = pattern.matcher(url.substring(Config.IOTDB_URL_PREFIX.length()));
    boolean isUrlLegal = false;
    while (matcher.find()) {
      params.setHost(matcher.group(1));
      params.setPort(Integer.parseInt(matcher.group(2)));
      isUrlLegal = true;
    }
    if (!isUrlLegal) {
      throw new IoTDBURLException("Error url format, url should be jdbc:iotdb://ip:port/");
    }

    if (info.containsKey(Config.AUTH_USER)) {
      params.setUsername(info.getProperty(Config.AUTH_USER));
    }
    if (info.containsKey(Config.AUTH_PASSWORD)) {
      params.setPassword(info.getProperty(Config.AUTH_PASSWORD));
    }

    return params;
  }

  /**
   * convert row records.
   */
  static List<RowRecord> convertRowRecords(TSQueryDataSet tsQueryDataSet,
      List<String> columnTypeList) {
    int rowCount = tsQueryDataSet.getRowCount();
    ByteBuffer byteBuffer = tsQueryDataSet.bufferForValues();

    // process time buffer
    List<RowRecord> rowRecordList = processTimeAndCreateRowRecords(byteBuffer, rowCount);

    for (String type : columnTypeList) {
      for (int i = 0; i < rowCount; i++) {
        Field field = null;
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
        rowRecordList.get(i).getFields().add(field);
      }
    }
    return rowRecordList;
  }

  private static List<RowRecord> processTimeAndCreateRowRecords(ByteBuffer byteBuffer,
      int rowCount) {
    List<RowRecord> rowRecordList = new ArrayList<>();
    for (int i = 0; i < rowCount; i++) {
      long timestamp = byteBuffer.getLong(); // byteBuffer has been flipped by the server side
      RowRecord rowRecord = new RowRecord(timestamp);
      rowRecordList.add(rowRecord);
    }
    return rowRecordList;
  }

}
