/**
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
import org.apache.iotdb.service.rpc.thrift.TSDataValue;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.service.rpc.thrift.TSRowRecord;
import org.apache.iotdb.service.rpc.thrift.TS_Status;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.utils.Binary;
import org.apache.iotdb.tsfile.utils.BytesUtils;
import org.apache.iotdb.tsfile.write.record.RowBatch;

/**
 * Utils to convert between thrift format and TsFile format.
 */
public class Utils {

  private static final int SUCCESS_CODE = 200;

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
   *
   * @param tsQueryDataSet -query data set
   * @return -list of row record
   */
  static List<RowRecord> convertRowRecords(TSQueryDataSet tsQueryDataSet) {
    List<RowRecord> records = new ArrayList<>();
    for (TSRowRecord ts : tsQueryDataSet.getRecords()) {
      RowRecord r = new RowRecord(ts.getTimestamp());
      int l = ts.getValuesSize();
      for (int i = 0; i < l; i++) {
        TSDataValue value = ts.getValues().get(i);
        if (value.is_empty) {
          Field field = new Field(null);
          field.setNull();
          r.getFields().add(field);
        } else {
          TSDataType dataType = TSDataType.valueOf(value.getType());
          Field field = new Field(dataType);
          addFieldAccordingToDataType(field, dataType, value);
          r.getFields().add(field);
        }
      }
      records.add(r);
    }
    return records;
  }

  /**
   *
   * @param field -the field need to add new data
   * @param dataType, -the data type of the new data
   * @param value, -the value of the new data
   */
  private static void addFieldAccordingToDataType(Field field, TSDataType dataType, TSDataValue value){
    switch (dataType) {
      case BOOLEAN:
        field.setBoolV(value.isBool_val());
        break;
      case INT32:
        field.setIntV(value.getInt_val());
        break;
      case INT64:
        field.setLongV(value.getLong_val());
        break;
      case FLOAT:
        field.setFloatV((float) value.getFloat_val());
        break;
      case DOUBLE:
        field.setDoubleV(value.getDouble_val());
        break;
      case TEXT:
        field.setBinaryV(new Binary(value.getBinary_val()));
        break;
      default:
        throw new UnSupportedDataTypeException(
                String.format("data type %s is not supported when convert data at client",
                        dataType));
    }
  }

}
