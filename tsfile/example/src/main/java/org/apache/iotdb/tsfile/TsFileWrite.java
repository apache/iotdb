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
import java.io.File;
import java.io.IOException;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.record.datapoint.DataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.FloatDataPoint;
import org.apache.iotdb.tsfile.write.record.datapoint.IntDataPoint;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
/**
 * An example of writing data to TsFile
 */
public class TsFileWrite {
  private static void writeData(TsFileWriter tsFileWriter) throws IOException, WriteProcessException {
    // construct TSRecord
    TSRecord tsRecord = new TSRecord(1, "device_1");
    DataPoint dPoint1 = new FloatDataPoint("sensor_1", 1.2f);
    DataPoint dPoint2 = new IntDataPoint("sensor_2", 20);
    DataPoint dPoint3;
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);

    // write a TSRecord to TsFile
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(2, "device_1");
    dPoint2 = new IntDataPoint("sensor_2", 20);
    dPoint3 = new IntDataPoint("sensor_3", 50);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(3, "device_1");
    dPoint1 = new FloatDataPoint("sensor_1", 1.4f);
    dPoint2 = new IntDataPoint("sensor_2", 21);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(4, "device_1");
    dPoint1 = new FloatDataPoint("sensor_1", 1.2f);
    dPoint2 = new IntDataPoint("sensor_2", 20);
    dPoint3 = new IntDataPoint("sensor_3", 51);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(6, "device_1");
    dPoint1 = new FloatDataPoint("sensor_1", 7.2f);
    dPoint2 = new IntDataPoint("sensor_2", 10);
    dPoint3 = new IntDataPoint("sensor_3", 11);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(7, "device_1");
    dPoint1 = new FloatDataPoint("sensor_1", 6.2f);
    dPoint2 = new IntDataPoint("sensor_2", 20);
    dPoint3 = new IntDataPoint("sensor_3", 21);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    tsRecord = new TSRecord(8, "device_1");
    dPoint1 = new FloatDataPoint("sensor_1", 9.2f);
    dPoint2 = new IntDataPoint("sensor_2", 30);
    dPoint3 = new IntDataPoint("sensor_3", 31);
    tsRecord.addTuple(dPoint1);
    tsRecord.addTuple(dPoint2);
    tsRecord.addTuple(dPoint3);
    tsFileWriter.write(tsRecord);

    // close TsFile
    tsFileWriter.close();
  }
  /**
   * There are two ways to construct a TsFile instance,they generate the identical TsFile file.
   * This method uses the first interface:
   * public void addMeasurementByJson(JSONObject measurement) throws WriteProcessException
   * The corresponding json string is provided below.
   * {
   *     "schema": [
   *         {
   *             "measurement_id": "sensor_1",
   *             "data_type": "FLOAT",
   *             "encoding": "RLE",
   * 		       "compressor" : "UNCOMPRESSED"
   *         },
   *         {
   *             "measurement_id": "sensor_2",
   *             "data_type": "INT32",
   *             "encoding": "TS_2DIFF",
   * 	            "compressor" : "UNCOMPRESSED"
   *
   *         },
   *         {
   *             "measurement_id": "sensor_3",
   *             "data_type": "INT32",
   *             "encoding": "TS_2DIFF",
   * 	           "compressor" : "UNCOMPRESSED"
   *
   *        }
   *     ]
   * }
   */
  private static void tsFileWriteWithJson() throws IOException,WriteProcessException {
    String path = "testWithJson.tsfile";
    String jsonText = "{\n" +
            "    \"schema\": [\n" +
            "        {\n" +
            "            \"measurement_id\": \"sensor_1\",\n" +
            "            \"data_type\": \"FLOAT\",\n" +
            "            \"encoding\": \"RLE\",\n" +
            "            \"compressor\" : \"UNCOMPRESSED\"\n" +
            "        },\n" +
            "        {\n" +
            "            \"measurement_id\": \"sensor_2\",\n" +
            "            \"data_type\": \"INT32\",\n" +
            "            \"encoding\": \"TS_2DIFF\",\n" +
            "            \"compressor\" : \"UNCOMPRESSED\"\n" +
            "\n" +
            "        },\n" +
            "        {\n" +
            "            \"measurement_id\": \"sensor_3\",\n" +
            "            \"data_type\": \"INT32\",\n" +
            "            \"encoding\": \"TS_2DIFF\",        \n" +
            "            \"compressor\" : \"UNCOMPRESSED\"\n" +
            "\n" +
            "  }\n" +
            "    ]\n" +
            "}";
    File f = new File(path);
    if (f.exists()) {
      f.delete();
    }
    TsFileWriter tsFileWriter = new TsFileWriter(f);
    JSONObject j = JSONObject.parseObject(jsonText);
    JSONArray schemas = j.getJSONArray("schema");
    // add measurements into file schema
    for (int i = 0; i < schemas.size(); ++i) {
      tsFileWriter.addMeasurementByJson(schemas.getJSONObject(i));
    }
    writeData(tsFileWriter);
  }

  /**
   * There are two ways to construct a TsFile instance,they generate the identical TsFile file.
   * This method uses the second interface:
   * public void addMeasurement(MeasurementSchema MeasurementSchema) throws WriteProcessException
   * The measurements are identical to the json string provided above.
   */
  private static void tsFileWriteDirect() throws IOException,WriteProcessException {
    String path = "testDirect.tsfile";
    File f = new File(path);
    if (f.exists()) {
      f.delete();
    }
    TsFileWriter tsFileWriter = new TsFileWriter(f);

    // add measurements into file schema
    tsFileWriter
            .addMeasurement(new MeasurementSchema("sensor_1", TSDataType.FLOAT, TSEncoding.RLE));
    tsFileWriter
            .addMeasurement(new MeasurementSchema("sensor_2", TSDataType.INT32, TSEncoding.TS_2DIFF));
    tsFileWriter
            .addMeasurement(new MeasurementSchema("sensor_3", TSDataType.INT32, TSEncoding.TS_2DIFF));
    writeData(tsFileWriter);
  }

  public static void main(String args[]) {
    try {
        // Use a json string for all the measurements to write a TsFile
      tsFileWriteWithJson();
      // Write a TsFile by adding the measurements directly in the method
      tsFileWriteDirect();
    } catch (Throwable e) {
      e.printStackTrace();
      System.out.println(e.getMessage());
    }
  }
}
