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

package org.apache.iotdb.commons.utils;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.write.TsFileWriter;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collections;

public class FileUtilsTest {
  private File tmpDir;
  private File targetDir;

  @Before
  public void setUp() throws Exception {
    tmpDir = new File(Files.createTempDirectory("load").toUri());
    targetDir = new File(Files.createTempDirectory("target").toUri());
  }

  @After
  public void tearDown() throws Exception {
    tmpDir.delete();
    targetDir.delete();
  }

  @Test
  public void testFileUtils() throws WriteProcessException, IOException {
    File tstFile = new File(tmpDir, "1-1-0-0.tsfile");
    File tstFile2 = new File(tmpDir, "2-1-0-0.tsfile");
    generateFile(tstFile);
    FileUtils.copyFile(tstFile, tstFile2);
    FileUtils.moveFileWithMD5Check(tstFile, targetDir);
    tstFile2.renameTo(tstFile);
    FileUtils.moveFileWithMD5Check(tstFile, targetDir);
  }

  private void generateFile(File tsfile) throws WriteProcessException, IOException {
    try (TsFileWriter writer = new TsFileWriter(tsfile)) {
      writer.registerAlignedTimeseries(
          "root.test.d1",
          Collections.singletonList(new MeasurementSchema("s1", TSDataType.BOOLEAN)));
      Tablet tablet =
          new Tablet(
              "root.test.d1",
              Collections.singletonList(new MeasurementSchema("s1", TSDataType.BOOLEAN)));
      for (int i = 0; i < 5; i++) {
        tablet.addTimestamp(i, i);
        tablet.addValue(i, 0, true);
      }
      writer.writeTree(tablet);
    }
  }
}
