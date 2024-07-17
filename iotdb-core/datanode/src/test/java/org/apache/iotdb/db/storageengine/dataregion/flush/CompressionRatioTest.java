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
package org.apache.iotdb.db.storageengine.dataregion.flush;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.utils.FilePathUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Locale;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class CompressionRatioTest {

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private CompressionRatio compressionRatio = CompressionRatio.getInstance();

  private static final String directory =
      FilePathUtils.regularizePath(CONFIG.getSystemDir()) + CompressionRatio.COMPRESSION_RATIO_DIR;

  @Before
  public void setUp() throws Exception {
    EnvironmentUtils.envSetUp();
    FileUtils.forceMkdir(new File(directory));
    compressionRatio.reset();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testCompressionRatio() throws IOException {
    long totalMemorySize = 10;
    long totalDiskSize = 5;

    for (int i = 0; i < 5; i++) {
      this.compressionRatio.updateRatio(10, 5);
      if (!new File(
              directory,
              String.format(
                  Locale.ENGLISH,
                  CompressionRatio.RATIO_FILE_PATH_FORMAT,
                  totalMemorySize,
                  totalDiskSize))
          .exists()) {
        fail();
      }
      assertEquals(2, this.compressionRatio.getRatio(), 0.1);
      totalMemorySize += 10;
      totalDiskSize += 5;
    }
  }

  @Test
  public void testRestore() throws IOException {
    Files.createFile(
        new File(
                directory,
                String.format(Locale.ENGLISH, CompressionRatio.RATIO_FILE_PATH_FORMAT, 1000, 100))
            .toPath());
    Files.createFile(
        new File(
                directory,
                String.format(Locale.ENGLISH, CompressionRatio.RATIO_FILE_PATH_FORMAT, 1000, 200))
            .toPath());
    Files.createFile(
        new File(
                directory,
                String.format(Locale.ENGLISH, CompressionRatio.RATIO_FILE_PATH_FORMAT, 1000, 500))
            .toPath());

    compressionRatio.restore();

    // if multiple files exist in the system due to some exceptions, restore the file with the
    // largest diskSize to the memory
    assertEquals(2, compressionRatio.getRatio(), 0.1);
  }

  @Test
  public void testRestoreIllegal1() throws IOException {
    Files.createFile(
        new File(
                directory,
                String.format(Locale.ENGLISH, CompressionRatio.RATIO_FILE_PATH_FORMAT, 10, 50))
            .toPath());

    Files.createFile(
        new File(
                directory,
                String.format(Locale.ENGLISH, CompressionRatio.RATIO_FILE_PATH_FORMAT, -1000, 100))
            .toPath());

    compressionRatio.restore();

    // if multiple files exist in the system due to some exceptions, restore the file with the
    // largest diskSize to the memory
    assertEquals(0.2, compressionRatio.getRatio(), 0.1);
  }

  @Test
  public void testRestoreIllegal2() throws IOException {

    Files.createFile(
        new File(
                directory,
                String.format(Locale.ENGLISH, CompressionRatio.RATIO_FILE_PATH_FORMAT, -1000, 100))
            .toPath());

    compressionRatio.restore();

    // if compression ratio from file is negative, assume the compression ratio is 0 / 0 = NaN
    assertEquals(Double.NaN, compressionRatio.getRatio(), 0.1);
  }
}
