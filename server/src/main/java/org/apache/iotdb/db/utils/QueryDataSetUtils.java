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
package org.apache.iotdb.db.utils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.iotdb.db.tools.watermark.WatermarkEncoder;
import org.apache.iotdb.service.rpc.thrift.TSDataValue;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.service.rpc.thrift.TSRowRecord;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;

/**
 * TimeValuePairUtils to convert between thrift format and TsFile format.
 */
public class QueryDataSetUtils {

  private QueryDataSetUtils(){}

  /**
   * convert query data set by fetch size.
   *
   * @param queryDataSet -query dataset
   * @param fetchSize -fetch size
   * @return -convert query dataset
   */
  public static TSQueryDataSet convertQueryDataSetByFetchSize(QueryDataSet queryDataSet,
      int fetchSize) throws IOException {
    return convertQueryDataSetByFetchSize(queryDataSet, fetchSize, null);
  }

  public static TSQueryDataSet convertQueryDataSetByFetchSize(QueryDataSet queryDataSet,
      int fetchSize, WatermarkEncoder watermarkEncoder) throws IOException {
    TSQueryDataSet tsQueryDataSet = new TSQueryDataSet();
    tsQueryDataSet.setRecords(new ArrayList<>());
    for (int i = 0; i < fetchSize; i++) {
      if (queryDataSet.hasNext()) {
        RowRecord rowRecord = queryDataSet.next();
        if (watermarkEncoder != null) {
          rowRecord = watermarkEncoder.encodeRecord(rowRecord);
        }
        tsQueryDataSet.getRecords().add(convertToTSRecord(rowRecord));
      } else {
        break;
      }
    }
    return tsQueryDataSet;
  }

  /**
   * convert to tsRecord.
   *
   * @param rowRecord -row record
   */
  private static TSRowRecord convertToTSRecord(RowRecord rowRecord) {
    TSRowRecord tsRowRecord = new TSRowRecord();
    tsRowRecord.setTimestamp(rowRecord.getTimestamp());
    tsRowRecord.setValues(new ArrayList<>());
    List<Field> fields = rowRecord.getFields();
    for (Field f : fields) {
      TSDataValue value = new TSDataValue(false);
      if (f.getDataType() == null) {
        value.setIs_empty(true);
      } else {
        switch (f.getDataType()) {
          case BOOLEAN:
            value.setBool_val(f.getBoolV());
            break;
          case INT32:
            value.setInt_val(f.getIntV());
            break;
          case INT64:
            value.setLong_val(f.getLongV());
            break;
          case FLOAT:
            value.setFloat_val(f.getFloatV());
            break;
          case DOUBLE:
            value.setDouble_val(f.getDoubleV());
            break;
          case TEXT:
            value.setBinary_val(ByteBuffer.wrap(f.getBinaryV().getValues()));
            break;
          default:
            throw new UnSupportedDataTypeException(String.format(
                "data type %s is not supported when convert data at server",
                f.getDataType().toString()));
        }
        value.setType(f.getDataType().toString());
      }
      tsRowRecord.getValues().add(value);
    }
    return tsRowRecord;
  }
}
