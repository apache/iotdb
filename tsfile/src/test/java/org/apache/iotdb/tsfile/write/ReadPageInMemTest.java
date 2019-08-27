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
package org.apache.iotdb.tsfile.write;

import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.exception.write.WriteProcessException;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.utils.RecordUtils;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.apache.iotdb.tsfile.write.schema.Schema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ReadPageInMemTest {

  private String filePath = "TsFileReadPageInMem";
  private File file = new File(filePath);
  private TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
  private TsFileWriter innerWriter;
  private Schema schema = null;

  private int pageSize;
  private int ChunkGroupSize;
  private int pageCheckSizeThreshold;
  private int defaultMaxStringLength;

  private static Schema getSchema() {
    Schema schema = new Schema();
    TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
    schema.registerMeasurement(new MeasurementSchema("s1", TSDataType.INT32, TSEncoding.valueOf(conf.valueEncoder)));
    schema.registerMeasurement(new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.valueOf(conf.valueEncoder)));
    schema.registerMeasurement(new MeasurementSchema("s3", TSDataType.FLOAT, TSEncoding.valueOf(conf.valueEncoder)));
    schema.registerMeasurement(new MeasurementSchema("s4", TSDataType.DOUBLE, TSEncoding.valueOf(conf.valueEncoder)));
    return schema;
  }

  @Before
  public void setUp() throws Exception {
    file.delete();
    pageSize = conf.pageSizeInByte;
    conf.pageSizeInByte = 200;
    ChunkGroupSize = conf.groupSizeInByte;
    conf.groupSizeInByte = 100000;
    pageCheckSizeThreshold = conf.pageCheckSizeThreshold;
    conf.pageCheckSizeThreshold = 1;
    defaultMaxStringLength = conf.maxStringLength;
    conf.maxStringLength = 2;
    schema = getSchema();
    innerWriter = new TsFileWriter(new File(filePath), schema, conf);
  }

  @After
  public void tearDown() throws Exception {
    file.delete();
    conf.pageSizeInByte = pageSize;
    conf.groupSizeInByte = ChunkGroupSize;
    conf.pageCheckSizeThreshold = pageCheckSizeThreshold;
    conf.maxStringLength = defaultMaxStringLength;
  }

  @Test
  public void OneDeviceTest() {
    String line = "";
    for (int i = 1; i <= 3; i++) {
      line = "root.car.d1," + i + ",s1,1,s2,1,s3,0.1,s4,0.1";
      TSRecord record = RecordUtils.parseSimpleTupleRecord(line, schema);
      try {
        innerWriter.write(record);
      } catch (IOException | WriteProcessException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    }
    for (int i = 4; i < 100; i++) {
      line = "root.car.d1," + i + ",s1,1,s2,1,s3,0.1,s4,0.1";
      TSRecord record = RecordUtils.parseSimpleTupleRecord(line, schema);
      try {
        innerWriter.write(record);
      } catch (IOException | WriteProcessException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    }
    try {
      innerWriter.close();
    } catch (IOException e) {
      e.printStackTrace();
      fail(e.getMessage());
    }
  }

  @Test
  public void MultiDeviceTest() throws IOException {

    String line = "";
    for (int i = 1; i <= 3; i++) {
      line = "root.car.d1," + i + ",s1,1,s2,1,s3,0.1,s4,0.1";
      TSRecord record = RecordUtils.parseSimpleTupleRecord(line, schema);
      try {
        innerWriter.write(record);
      } catch (IOException | WriteProcessException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    }
    for (int i = 1; i <= 3; i++) {
      line = "root.car.d2," + i + ",s1,1,s2,1,s3,0.1,s4,0.1";
      TSRecord record = RecordUtils.parseSimpleTupleRecord(line, schema);
      try {
        innerWriter.write(record);
      } catch (IOException | WriteProcessException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    }

    for (int i = 4; i < 100; i++) {
      line = "root.car.d1," + i + ",s1,1,s2,1,s3,0.1,s4,0.1";
      TSRecord record = RecordUtils.parseSimpleTupleRecord(line, schema);
      try {
        innerWriter.write(record);
      } catch (IOException | WriteProcessException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    }

    for (int i = 4; i < 100; i++) {
      line = "root.car.d2," + i + ",s1,1,s2,1,s3,0.1,s4,0.1";
      TSRecord record = RecordUtils.parseSimpleTupleRecord(line, schema);
      try {
        innerWriter.write(record);
      } catch (IOException | WriteProcessException e) {
        e.printStackTrace();
        fail(e.getMessage());
      }
    }

    innerWriter.close();
  }
}
