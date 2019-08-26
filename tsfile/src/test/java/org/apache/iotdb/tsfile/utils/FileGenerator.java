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
package org.apache.iotdb.tsfile.utils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Scanner;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.TsFileWriter;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileGenerator {

  private static final Logger LOG = LoggerFactory.getLogger(FileGenerator.class);
  public static int ROW_COUNT = 1000;
  public static TsFileWriter innerWriter;
  public static String inputDataFile;
  public static String outputDataFile = "target/perTestOutputData.tsfile";
  public static String errorOutputDataFile;
  public static Schema schema;
  public static int oldMaxNumberOfPointsInPage;

  public static void generateFile(int rowCount, int maxNumberOfPointsInPage)
      throws IOException, InterruptedException, WriteProcessException {
    ROW_COUNT = rowCount;
    TSFileConfig config = TSFileDescriptor.getInstance().getConfig();
    oldMaxNumberOfPointsInPage = config.maxNumberOfPointsInPage;
    config.maxNumberOfPointsInPage = maxNumberOfPointsInPage;

    prepare();
    write();
    config.maxNumberOfPointsInPage = oldMaxNumberOfPointsInPage;
  }

  public static void generateFile()
      throws IOException, InterruptedException, WriteProcessException {
    generateFile(1000, 10);
  }

  public static void prepare() throws IOException {
    inputDataFile = "target/perTestInputData";
    errorOutputDataFile = "target/perTestErrorOutputData.tsfile";
    generateTestData();
    generateSampleInputDataFile();
  }

  public static void after() {
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

    long startTime = 1480562618000L;
    startTime = startTime - startTime % 1000;
    for (int i = 0; i < ROW_COUNT; i++) {
      // write d1
      String d1 = "d1," + (startTime + i) + ",s1," + (i * 10 + 1) + ",s2," + (i * 10 + 2);
      if (i % 20 < 10) {
        // LOG.info("write null to d1:" + (startTime + i));
        d1 = "d1," + (startTime + i) + ",s1,,s2," + (i * 10 + 2);
      }
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
        "d2,3," + (startTime + ROW_COUNT) + ",s2," + (ROW_COUNT * 10 + 2) + ",s3," + (ROW_COUNT * 10
            + 3);
    fw.write(d + "\r\n");
    d = "d2," + (startTime + ROW_COUNT + 1) + ",2,s-1," + (ROW_COUNT * 10 + 2);
    fw.write(d + "\r\n");
    fw.close();
  }

  static public void write() throws IOException {
    File file = new File(outputDataFile);
    File errorFile = new File(errorOutputDataFile);
    if (file.exists()) {
      file.delete();
    }
    if (errorFile.exists()) {
      errorFile.delete();
    }

    innerWriter = new TsFileWriter(file, schema, TSFileDescriptor.getInstance().getConfig());

    // write
    try {
      writeToFile(schema);
    } catch (WriteProcessException e) {
      e.printStackTrace();
    }
    LOG.info("write to file successfully!!");
  }

  private static void generateTestData() {
    schema = new Schema();
    TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
    schema.registerMeasurement(new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.valueOf(conf.valueEncoder)));
    schema.registerMeasurement(new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.valueOf(conf.valueEncoder)));
    schema.registerMeasurement(new MeasurementSchema("s3", TSDataType.INT64, TSEncoding.valueOf(conf.valueEncoder)));
    schema.registerMeasurement(new MeasurementSchema("s4", TSDataType.TEXT, TSEncoding.PLAIN));
    schema.registerMeasurement(new MeasurementSchema("s5", TSDataType.BOOLEAN, TSEncoding.PLAIN));
    schema.registerMeasurement(new MeasurementSchema("s6", TSDataType.FLOAT, TSEncoding.RLE));
    schema.registerMeasurement(new MeasurementSchema("s7", TSDataType.DOUBLE, TSEncoding.RLE));
  }

  static public void writeToFile(Schema schema)
      throws IOException, WriteProcessException {
    Scanner in = getDataFile(inputDataFile);
    long lineCount = 0;
    long startTime = System.currentTimeMillis();
    long endTime = System.currentTimeMillis();
    assert in != null;
    while (in.hasNextLine()) {
      if (lineCount % 1000000 == 0) {
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
