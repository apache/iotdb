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
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.utils.TestHelper;
import org.apache.iotdb.tsfile.file.metadata.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ChunkMetaDataTest {

  public static final String MEASUREMENT_UID = "sensor231";
  public static final long FILE_OFFSET = 2313424242L;
  public static final long NUM_OF_POINTS = 123456L;
  public static final long START_TIME = 523372036854775806L;
  public static final long END_TIME = 523372036854775806L;
  public static final TSDataType DATA_TYPE = TSDataType.INT64;
  final String PATH = "target/outputTimeSeriesChunk.tsfile";

  @Before
  public void setUp() {
  }

  @After
  public void tearDown() {
    File file = new File(PATH);
    if (file.exists()) {
      file.delete();
    }
  }

  @Test
  public void testWriteIntoFile() throws IOException {
    ChunkMetaData metaData = TestHelper.createSimpleTimeSeriesChunkMetaData();
    serialized(metaData);
    ChunkMetaData readMetaData = deSerialized();
    Utils.isTimeSeriesChunkMetadataEqual(metaData, readMetaData);
    serialized(readMetaData);
  }

  private ChunkMetaData deSerialized() {
    FileInputStream fis = null;
    ChunkMetaData metaData = null;
    try {
      fis = new FileInputStream(new File(PATH));
      metaData = ChunkMetaData.deserializeFrom(fis);
      return metaData;
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (fis != null) {
        try {
          fis.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
    return metaData;
  }

  private void serialized(ChunkMetaData metaData) {
    File file = new File(PATH);
    if (file.exists()) {
      file.delete();
    }
    FileOutputStream fos = null;
    try {
      fos = new FileOutputStream(file);
      metaData.serializeTo(fos);
    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (fos != null) {
        try {
          fos.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }
  }
}
