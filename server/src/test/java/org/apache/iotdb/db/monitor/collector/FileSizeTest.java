/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.monitor.collector;

import static org.junit.Assert.assertEquals;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import org.apache.iotdb.db.monitor.MonitorConstants.FileSizeConstants;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class FileSizeTest {

  private static final String TEST_FILE_CONTENT = "FileSize UT test file";
  private static final String TEST_FILE_PATH =
      FileSizeConstants.SYS.getPath() + File.separatorChar + "schemaFile";

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Ignore
  @Test
  public void testGetFileSizesInByte() {
    long dataSizeBefore;
    long dataSizeAfter;
    boolean isWriteSuccess = true;
    File testFile = new File(TEST_FILE_PATH);
    if (testFile.exists()) {
      try {
        Files.delete(testFile.toPath());
      } catch (IOException e) {
        isWriteSuccess = false;
        e.printStackTrace();
      }
    }
    try {
      if (!testFile.createNewFile()) {
        isWriteSuccess = false;
      }
    } catch (IOException e) {
      e.printStackTrace();
    }

    dataSizeBefore = FileSize.getInstance().getFileSizesInByte().get(FileSizeConstants.SYS);
    byte[] contentInBytes = TEST_FILE_CONTENT.getBytes();
    // insert something into the test file under data dir
    try (FileOutputStream fileOutputStream = new FileOutputStream(testFile)) {
      fileOutputStream.write(contentInBytes);
      fileOutputStream.flush();
    } catch (IOException e) {
      isWriteSuccess = false;
      e.printStackTrace();
    }
    // calculate the delta of data dir file size
    dataSizeAfter = FileSize.getInstance().getFileSizesInByte().get(FileSizeConstants.SYS);
    long deltaSize = dataSizeAfter - dataSizeBefore;

    if (isWriteSuccess) {
      //check if the the delta of data dir file size is equal to the written content size in byte
      assertEquals(contentInBytes.length, deltaSize);
    } else {
      assertEquals(0, deltaSize);
    }
  }
}
