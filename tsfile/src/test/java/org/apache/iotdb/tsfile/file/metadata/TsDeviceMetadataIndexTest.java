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
package org.apache.iotdb.tsfile.file.metadata;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.iotdb.tsfile.file.metadata.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TsDeviceMetadataIndexTest {

  private TsDeviceMetadataIndex index;

  private long offset = 10;
  private int len = 10;
  private long startTime = 100;
  private long endTime = 200;

  private File file;
  private String path = "target/TsDeviceMetadataIndex.tsfile";

  @Before
  public void setUp() {
    index = new TsDeviceMetadataIndex();
    index.setOffset(offset);
    index.setLen(len);
    index.setStartTime(startTime);
    index.setEndTime(endTime);
    file = new File(path);
  }

  @After
  public void tearDown() {
    file.delete();
  }

  @Test
  public void testSerDeDeviceMetadataIndex() throws IOException {
    OutputStream outputStream = new FileOutputStream(file);
    try {
      index.serializeTo(outputStream);
      InputStream inputStream = new FileInputStream(file);
      try {
        TsDeviceMetadataIndex index2 = TsDeviceMetadataIndex.deserializeFrom(inputStream);
        Utils.isTsDeviceMetadataIndexEqual(index, index2);
      } finally {
        inputStream.close();
      }
    } finally {
      outputStream.close();
    }
  }
}