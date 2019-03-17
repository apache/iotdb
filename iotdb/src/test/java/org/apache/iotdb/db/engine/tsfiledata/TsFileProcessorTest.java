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

package org.apache.iotdb.db.engine.tsfiledata;

import java.util.HashMap;
import java.util.Map;
import org.apache.iotdb.db.engine.bufferwrite.Action;
import org.apache.iotdb.db.engine.bufferwrite.ActionException;
import org.apache.iotdb.db.engine.version.SysTimeVersionController;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.apache.iotdb.tsfile.write.schema.FileSchema;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TsFileProcessorTest {
  TsFileProcessor processor;
  Action doNothingAction = new Action() {
    @Override
    public void act() throws ActionException {
    }
  };
  Map<String, MeasurementSchema> measurementSchemaMap = new HashMap<>();

  FileSchema schema;

  @Before
  public void setUp() throws Exception {
    measurementSchemaMap.put("s1", new MeasurementSchema("s1", TSDataType.FLOAT, TSEncoding.RLE));
    measurementSchemaMap.put("s1", new MeasurementSchema("s2", TSDataType.FLOAT, TSEncoding.RLE));
    measurementSchemaMap.put("s1", new MeasurementSchema("s3", TSDataType.FLOAT, TSEncoding.RLE));
    schema = new FileSchema(measurementSchemaMap);
    processor = new TsFileProcessor("root.test", doNothingAction, doNothingAction, doNothingAction,
        SysTimeVersionController.INSTANCE, schema);
  }


  @After
  public void tearDown() throws Exception {
    processor.close();
   // processor.removeMe();
  }

  @Test
  public void insert() {
  }

  @Test
  public void insert1() {
  }

  @Test
  public void delete() {
  }

  @Test
  public void flush() {
  }

  @Test
  public void canBeClosed() {
  }

  @Test
  public void close() {
  }

  @Test
  public void memoryUsage() {
  }

  @Test
  public void isFlush() {
  }

  @Test
  public void queryBufferWriteData() {
  }

  @Test
  public void getInsertFilePath() {
  }

  @Test
  public void getLogNode() {
  }

  @Test
  public void getLastFlushTime() {
  }

  @Test
  public void getFlushFuture() {
  }
}
