/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.tsfile.read.query.timegenerator;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.common.constant.JsonFormatConstant;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.FileUtils;
import org.apache.iotdb.tsfile.utils.FileUtils.Unit;
import org.apache.iotdb.tsfile.utils.RecordUtils;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Ignore;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Ignore
public class TsFileGeneratorForSeriesReaderByTimestamp {

  public static final long START_TIMESTAMP = 1480562618000L;
  private static final Logger LOG = LoggerFactory
      .getLogger(TsFileGeneratorForSeriesReaderByTimestamp.class);
  public static TsFileWriter innerWriter;
  public static String inputDataFile;
  public static String outputDataFile = "src/test/resources/testTsFile.tsfile";
  public static String errorOutputDataFile;
  public static JSONObject jsonSchema;
  private static int rowCount;
  private static int chunkGroupSize;
  private static int pageSize;
  private static int preChunkGroupSize;
  private static int prePageSize;

  public static void generateFile(int rc, int rs, int ps)
      throws IOException, InterruptedException, WriteProcessException {
    rowCount = rc;
    chunkGroupSize = rs;
    pageSize = ps;
    prepare();
    write();
  }

  public static void prepare() throws IOException {
    inputDataFile = "src/test/resources/perTestInputData";
    errorOutputDataFile = "src/test/resources/perTestErrorOutputData.tsfile";
    jsonSchema = generateTestData();
    generateSampleInputDataFile();
  }

  public static void after() {
    TSFileDescriptor.getInstance().getConfig().groupSizeInByte = preChunkGroupSize;
    TSFileDescriptor.getInstance().getConfig().maxNumberOfPointsInPage = prePageSize;
    File file = new File(inputDataFile);
    if (file.exists()) {
      file.delete();
    }
    file = new File(outputDataFile);
    if (file.exists()) {
      file.delete();
    }
    file = new File(errorOutputDataFile);
    if (file.exists()) {
      file.delete();
    }
  }

  static private void generateSampleInputDataFile() throws IOException {
    File file = new File(inputDataFile);
    if (file.exists()) {
      file.delete();
    }
    file.getParentFile().mkdirs();
    FileWriter fw = new FileWriter(file);

    long startTime = START_TIMESTAMP;
    for (int i = 0; i < rowCount; i += 2) {
      // write d1
      String d1 = "d1," + (startTime + i) + ",s1," + (i * 10 + 1) + ",s2," + (i * 10 + 2);
      if (i % 5 == 0) {
        d1 += ",s3," + (i * 10 + 3);
      }
      if (i % 8 == 0) {
        d1 += ",s4," + "dog" + i;
      }
      if (i % 9 == 0) {
        d1 += ",s5," + "false";
      }
      if (i % 10 == 0) {
        d1 += ",s6," + ((int) (i / 9.0) * 100) / 100.0;
      }
      if (i % 11 == 0) {
        d1 += ",s7," + ((int) (i / 10.0) * 100) / 100.0;
      }
      fw.write(d1 + "\r\n");

      // write d2
      String d2 = "d2," + (startTime + i) + ",s2," + (i * 10 + 2) + ",s3," + (i * 10 + 3);
      if (i % 20 < 5) {
        // LOG.info("write null to d2:" + (startTime + i));
        d2 = "d2," + (startTime + i) + ",s2,,s3," + (i * 10 + 3);
      }
      if (i % 5 == 0) {
        d2 += ",s1," + (i * 10 + 1);
      }
      if (i % 8 == 0) {
        d2 += ",s4," + "dog" + i % 4;
      }
      fw.write(d2 + "\r\n");
    }
    // write error
    String d =
        "d2,3," + (startTime + rowCount) + ",s2," + (rowCount * 10 + 2) + ",s3," + (rowCount * 10
            + 3);
    fw.write(d + "\r\n");
    d = "d2," + (startTime + rowCount + 1) + ",2,s-1," + (rowCount * 10 + 2);
    fw.write(d + "\r\n");
    fw.close();
  }

  static public void write() throws IOException, InterruptedException, WriteProcessException {
    File file = new File(outputDataFile);
    File errorFile = new File(errorOutputDataFile);
    if (file.exists()) {
      file.delete();
    }
    if (errorFile.exists()) {
      errorFile.delete();
    }

    // LOG.info(jsonSchema.toString());
    FileSchema schema = new FileSchema(jsonSchema);
    preChunkGroupSize = TSFileDescriptor.getInstance().getConfig().groupSizeInByte;
    prePageSize = TSFileDescriptor.getInstance().getConfig().maxNumberOfPointsInPage;
    TSFileDescriptor.getInstance().getConfig().groupSizeInByte = chunkGroupSize;
    TSFileDescriptor.getInstance().getConfig().maxNumberOfPointsInPage = pageSize;
    innerWriter = new TsFileWriter(file, schema, TSFileDescriptor.getInstance().getConfig());

    // write
    try {
      writeToFile(schema);
    } catch (WriteProcessException e) {
      e.printStackTrace();
    }
    LOG.info("write to file successfully!!");
  }

  private static JSONObject generateTestData() {
    TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
    JSONObject s1 = new JSONObject();
    s1.put(JsonFormatConstant.MEASUREMENT_UID, "s1");
    s1.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT32.toString());
    s1.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.valueEncoder);
    s1.put(JsonFormatConstant.COMPRESS_TYPE, conf.compressor);
    JSONObject s2 = new JSONObject();
    s2.put(JsonFormatConstant.MEASUREMENT_UID, "s2");
    s2.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT64.toString());
    s2.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.valueEncoder);
    s2.put(JsonFormatConstant.COMPRESS_TYPE, "SNAPPY");
    JSONObject s3 = new JSONObject();
    s3.put(JsonFormatConstant.MEASUREMENT_UID, "s3");
    s3.put(JsonFormatConstant.DATA_TYPE, TSDataType.INT64.toString());
    s3.put(JsonFormatConstant.MEASUREMENT_ENCODING, conf.valueEncoder);
    s2.put(JsonFormatConstant.COMPRESS_TYPE, "UNCOMPRESSED");
    JSONObject s4 = new JSONObject();
    s4.put(JsonFormatConstant.MEASUREMENT_UID, "s4");
    s4.put(JsonFormatConstant.DATA_TYPE, TSDataType.TEXT.toString());
    s4.put(JsonFormatConstant.MEASUREMENT_ENCODING, TSEncoding.PLAIN.toString());
    JSONObject s5 = new JSONObject();
    s5.put(JsonFormatConstant.MEASUREMENT_UID, "s5");
    s5.put(JsonFormatConstant.DATA_TYPE, TSDataType.BOOLEAN.toString());
    s5.put(JsonFormatConstant.MEASUREMENT_ENCODING, TSEncoding.PLAIN.toString());
    JSONObject s6 = new JSONObject();
    s6.put(JsonFormatConstant.MEASUREMENT_UID, "s6");
    s6.put(JsonFormatConstant.DATA_TYPE, TSDataType.FLOAT.toString());
    s6.put(JsonFormatConstant.MEASUREMENT_ENCODING, TSEncoding.RLE.toString());
    JSONObject s7 = new JSONObject();
    s7.put(JsonFormatConstant.MEASUREMENT_UID, "s7");
    s7.put(JsonFormatConstant.DATA_TYPE, TSDataType.DOUBLE.toString());
    s7.put(JsonFormatConstant.MEASUREMENT_ENCODING, TSEncoding.RLE.toString());

    JSONArray measureGroup1 = new JSONArray();
    measureGroup1.put(s1);
    measureGroup1.put(s2);
    measureGroup1.put(s3);
    measureGroup1.put(s4);
    measureGroup1.put(s5);
    measureGroup1.put(s6);
    measureGroup1.put(s7);

    JSONObject jsonSchema = new JSONObject();
    jsonSchema.put(JsonFormatConstant.DELTA_TYPE, "test_type");
    jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, measureGroup1);
    // System.out.println(jsonSchema);
    return jsonSchema;
  }

  static public void writeToFile(FileSchema schema)
      throws InterruptedException, IOException, WriteProcessException {
    Scanner in = getDataFile(inputDataFile);
    long lineCount = 0;
    long startTime = System.currentTimeMillis();
    long endTime = System.currentTimeMillis();
    assert in != null;
    while (in.hasNextLine()) {
      if (lineCount % 1000000 == 0) {
        endTime = System.currentTimeMillis();
        // logger.info("write line:{},inner space consumer:{},use
        // time:{}",lineCount,innerWriter.calculateMemSizeForEachGroup(),endTime);
        LOG.info("write line:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
      }
      String str = in.nextLine();
      TSRecord record = RecordUtils.parseSimpleTupleRecord(str, schema);
      innerWriter.write(record);
      lineCount++;
    }
    endTime = System.currentTimeMillis();
    LOG.info("write line:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
    innerWriter.close();
    in.close();
    endTime = System.currentTimeMillis();
    LOG.info("write total:{},use time:{}s", lineCount, (endTime - startTime) / 1000);
    LOG.info("src file size:{}GB", FileUtils.getLocalFileByte(inputDataFile, Unit.GB));
    LOG.info("src file size:{}MB", FileUtils.getLocalFileByte(outputDataFile, Unit.MB));
  }

  static private Scanner getDataFile(String path) {
    File file = new File(path);
    try {
      Scanner in = new Scanner(file);
      return in;
    } catch (FileNotFoundException e) {
      e.printStackTrace();
      return null;
    }
  }
}
