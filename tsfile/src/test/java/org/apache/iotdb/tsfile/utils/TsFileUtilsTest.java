/*
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

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.constant.TestConstant;
import org.apache.iotdb.tsfile.write.writer.LocalTsFileOutput;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;

public class TsFileUtilsTest {
  private static final String COMPLETE_FILE_PATH =
      TestConstant.BASE_OUTPUT_PATH.concat("TsFileUtilsTest_Complete.tsfile");
  private static final String INCOMPLETE_FILE_PATH =
      TestConstant.BASE_OUTPUT_PATH.concat("TsFileUtilsTest_Incomplete.tsfile");

  @Before
  public void before() throws IOException {
    TsFileIOWriter completeWriter = new TsFileIOWriter(new File(COMPLETE_FILE_PATH));
    completeWriter.endFile();
    LocalTsFileOutput output =
        new LocalTsFileOutput(new FileOutputStream(new File(INCOMPLETE_FILE_PATH)));
    byte[] MAGIC_STRING_BYTES = BytesUtils.stringToBytes(TSFileConfig.MAGIC_STRING);
    byte MAGIC_NUMBER_BYTE = TSFileConfig.VERSION_NUMBER;
    // only write 1. version number 2. magic string
    output.write(MAGIC_NUMBER_BYTE);
    output.write(MAGIC_STRING_BYTES);
    output.close();
  }

  @After
  public void after() {
    File completeFile = new File(COMPLETE_FILE_PATH);
    File incompleteFile = new File(INCOMPLETE_FILE_PATH);
    if (completeFile.exists()) {
      completeFile.delete();
    }
    if (incompleteFile.exists()) {
      completeFile.delete();
    }
  }

  @Test
  public void isTsFileCompleteTest() throws IOException {
    Assert.assertTrue(TsFileUtils.isTsFileComplete(new File(COMPLETE_FILE_PATH)));
    Assert.assertFalse(TsFileUtils.isTsFileComplete(new File(INCOMPLETE_FILE_PATH)));
  }
}
