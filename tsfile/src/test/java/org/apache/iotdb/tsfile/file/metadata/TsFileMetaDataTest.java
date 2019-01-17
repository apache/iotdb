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
import org.apache.iotdb.tsfile.file.metadata.utils.TestHelper;
import org.apache.iotdb.tsfile.file.metadata.utils.Utils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class TsFileMetaDataTest {

  public static final int VERSION = 123;
  public static final String CREATED_BY = "tsf";
  final String PATH = "target/output1.tsfile";

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
  public void testWriteFileMetaData() throws IOException {
    TsFileMetaData tsfMetaData = TestHelper.createSimpleFileMetaData();
    serialized(tsfMetaData);
    TsFileMetaData readMetaData = deSerialized();
    Utils.isFileMetaDataEqual(tsfMetaData, readMetaData);
    serialized(readMetaData);
  }

  private TsFileMetaData deSerialized() {
    FileInputStream fis = null;
    TsFileMetaData metaData = null;
    try {
      fis = new FileInputStream(new File(PATH));
      metaData = TsFileMetaData.deserializeFrom(fis);
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

  private void serialized(TsFileMetaData metaData) {
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
