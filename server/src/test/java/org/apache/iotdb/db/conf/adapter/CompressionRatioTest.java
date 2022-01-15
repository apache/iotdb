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
package org.apache.iotdb.db.conf.adapter;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.utils.FilePathUtils;

import org.apache.commons.io.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
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
    EnvironmentUtils.closeStatMonitor();
    EnvironmentUtils.envSetUp();
    FileUtils.forceMkdir(new File(directory));
    compressionRatio.reset();
    compressionRatio.restore();
  }

  @After
  public void tearDown() throws Exception {
    EnvironmentUtils.cleanEnv();
  }

  @Test
  public void testCompressionRatio() throws IOException {
    double compressionRatioSum = 0;
    int calcuTimes = 0;
    if (new File(
            directory,
            String.format(
                Locale.ENGLISH,
                CompressionRatio.RATIO_FILE_PATH_FORMAT,
                compressionRatioSum,
                calcuTimes))
        .exists()) {
      fail();
    }
    double compressionRatio = 10;
    for (int i = 0; i < 500; i += compressionRatio) {
      this.compressionRatio.updateRatio(compressionRatio);
      if (new File(
              directory,
              String.format(
                  Locale.ENGLISH,
                  CompressionRatio.RATIO_FILE_PATH_FORMAT,
                  compressionRatioSum,
                  calcuTimes))
          .exists()) {
        fail();
      }
      calcuTimes++;
      compressionRatioSum += compressionRatio;
      if (!new File(
              directory,
              String.format(
                  Locale.ENGLISH,
                  CompressionRatio.RATIO_FILE_PATH_FORMAT,
                  compressionRatioSum,
                  calcuTimes))
          .exists()) {
        fail();
      }
      assertEquals(
          0, Double.compare(compressionRatioSum / calcuTimes, this.compressionRatio.getRatio()));
    }
  }

  @Test
  public void testRestore() throws IOException {
    double compressionRatioSum = 0;
    int calcuTimes = 0;
    if (new File(
            directory,
            String.format(
                Locale.ENGLISH,
                CompressionRatio.RATIO_FILE_PATH_FORMAT,
                compressionRatioSum,
                calcuTimes))
        .exists()) {
      fail();
    }
    int compressionRatio = 10;
    for (int i = 0; i < 100; i += compressionRatio) {
      this.compressionRatio.updateRatio(compressionRatio);
      if (new File(
              directory,
              String.format(
                  Locale.ENGLISH,
                  CompressionRatio.RATIO_FILE_PATH_FORMAT,
                  compressionRatioSum,
                  calcuTimes))
          .exists()) {
        fail();
      }
      calcuTimes++;
      compressionRatioSum += compressionRatio;
      if (!new File(
              directory,
              String.format(
                  Locale.ENGLISH,
                  CompressionRatio.RATIO_FILE_PATH_FORMAT,
                  compressionRatioSum,
                  calcuTimes))
          .exists()) {
        fail();
      }
      assertEquals(
          0, Double.compare(compressionRatioSum / calcuTimes, this.compressionRatio.getRatio()));
    }
    this.compressionRatio.restore();
    assertEquals(10, this.compressionRatio.getCalcTimes());
    assertEquals(
        0, Double.compare(compressionRatioSum / calcuTimes, this.compressionRatio.getRatio()));
  }
}
