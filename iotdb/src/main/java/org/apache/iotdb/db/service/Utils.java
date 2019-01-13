/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.service;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.iotdb.db.metadata.ColumnSchema;
import org.apache.iotdb.service.rpc.thrift.TSColumnSchema;
import org.apache.iotdb.service.rpc.thrift.TSDataValue;
import org.apache.iotdb.service.rpc.thrift.TSQueryDataSet;
import org.apache.iotdb.service.rpc.thrift.TSRowRecord;
import org.apache.iotdb.tsfile.exception.write.UnSupportedDataTypeException;
import org.apache.iotdb.tsfile.read.common.Field;
import org.apache.iotdb.tsfile.read.common.RowRecord;
import org.apache.iotdb.tsfile.read.query.dataset.QueryDataSet;
import org.apache.iotdb.db.metadata.ColumnSchema;

/**
 * TimeValuePairUtils to convert between thrift format and TsFile format
 */
public class Utils {

    public static Map<String, List<TSColumnSchema>> convertAllSchema(Map<String, List<ColumnSchema>> allSchema) {
        if (allSchema == null) {
            return null;
        }
        Map<String, List<TSColumnSchema>> tsAllSchema = new HashMap<>();
        for (Map.Entry<String, List<ColumnSchema>> entry : allSchema.entrySet()) {
            List<TSColumnSchema> tsColumnSchemas = new ArrayList<>();
            for (ColumnSchema columnSchema : entry.getValue()) {
                tsColumnSchemas.add(convertColumnSchema(columnSchema));
            }
            tsAllSchema.put(entry.getKey(), tsColumnSchemas);
        }
        return tsAllSchema;
    }

    private static TSColumnSchema convertColumnSchema(ColumnSchema schema) {
        if (schema == null) {
            return null;
        }
        TSColumnSchema tsColumnSchema = new TSColumnSchema();
        tsColumnSchema.setName(schema.name);
        tsColumnSchema.setDataType(schema.dataType == null ? null : schema.dataType.toString());
        tsColumnSchema.setEncoding(schema.encoding == null ? null : schema.encoding.toString());
        tsColumnSchema.setOtherArgs(schema.getArgsMap() == null ? null : schema.getArgsMap());
        return tsColumnSchema;
    }

    public static TSQueryDataSet convertQueryDataSetByFetchSize(QueryDataSet queryDataSet, int fetchsize)
            throws IOException {
        TSQueryDataSet tsQueryDataSet = new TSQueryDataSet();
        tsQueryDataSet.setRecords(new ArrayList<>());
        for (int i = 0; i < fetchsize; i++) {
            if (queryDataSet.hasNext()) {
                RowRecord rowRecord = queryDataSet.next();
                tsQueryDataSet.getRecords().add(convertToTSRecord(rowRecord));
            } else {
                break;
            }
        }
        return tsQueryDataSet;
    }

    public static TSRowRecord convertToTSRecord(RowRecord rowRecord) {
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
                    value.setBinary_val(ByteBuffer.wrap(f.getBinaryV().values));
                    break;
                default:
                    throw new UnSupportedDataTypeException(String.format(
                            "data type %s is not supported when convert data at server", f.getDataType().toString()));
                }
                value.setType(f.getDataType().toString());
            }
            tsRowRecord.getValues().add(value);
        }
        return tsRowRecord;
    }
}
