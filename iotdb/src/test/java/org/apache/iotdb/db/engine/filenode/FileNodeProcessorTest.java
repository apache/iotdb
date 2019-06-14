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

package org.apache.iotdb.db.engine.filenode;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.MetadataManagerHelper;
import org.apache.iotdb.db.engine.bufferwrite.BufferWriteProcessor;
import org.apache.iotdb.db.exception.BufferWriteProcessorException;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.FileNodeProcessorException;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.record.TSRecord;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class FileNodeProcessorTest {

  FileNodeProcessor processor;
  private String deviceId = "root.vehicle.d0";
  private String measurementId = "s0";
  private TSDataType dataType = TSDataType.INT32;
  private String processName = "root.vehicle";
  @Before
  public void setUp() throws FileNodeProcessorException {
    // init metadata
    MetadataManagerHelper.initMetadata();
    processor = new FileNodeProcessor(IoTDBDescriptor.getInstance().getConfig().getFileNodeDir(), processName);

  }

  @After
  public void tearDown() throws IOException, FileNodeManagerException {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testAsyncClose()
      throws FileNodeProcessorException, BufferWriteProcessorException, ExecutionException, InterruptedException {

    BufferWriteProcessor bwProcessor;
    int i =1;
    for (int j = 1; j < 5; j++) {
      bwProcessor = processor.getBufferWriteProcessor(processName, System.currentTimeMillis());
      for (; i <= 100 * j; i++) {
        bwProcessor.write(deviceId, measurementId, i, TSDataType.INT32, String.valueOf(i));
      }
      processor.closeBufferWrite();
    }
    Assert.assertNotEquals(0, processor.getClosingBufferWriteProcessor().size());
    processor.waitforAllClosed();
    Assert.assertEquals(0, processor.getClosingBufferWriteProcessor().size());

  }
}
