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

package org.apache.iotdb.db.pipe.sink.util.builder;

import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.external.commons.io.FileUtils;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

public class PipeTableModelTsFileBuilderV2Test {

  private static File temporaryPipeReceiverDir;
  private static String[] originalPipeReceiverFileDirs;

  @BeforeClass
  public static void setUpClass() throws Exception {
    originalPipeReceiverFileDirs =
        IoTDBDescriptor.getInstance().getConfig().getPipeReceiverFileDirs();
    temporaryPipeReceiverDir = Files.createTempDirectory("pipe-table-builder-v2-test").toFile();
    IoTDBDescriptor.getInstance()
        .getConfig()
        .setPipeReceiverFileDirs(new String[] {temporaryPipeReceiverDir.getAbsolutePath()});
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setPipeReceiverFileDirs(originalPipeReceiverFileDirs);
    FileUtils.deleteDirectory(temporaryPipeReceiverDir);
  }

  @Test
  public void testFallbackBuilderConvertsBufferedTablets() throws Exception {
    final PipeTableModelTsFileBuilderV2 builder =
        new PipeTableModelTsFileBuilderV2(new AtomicLong(1), new AtomicLong(0)) {
          @Override
          protected File createFile() throws IOException {
            throw new IOException();
          }
        };
    final List<Pair<String, File>> fallbackFiles = new ArrayList<>();
    try {
      final Tablet tablet =
          new Tablet(
              "table",
              Arrays.asList("tag", "field"),
              Arrays.asList(TSDataType.STRING, TSDataType.INT64),
              Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
              1);
      tablet.addTimestamp(0, 1);
      tablet.addValue(0, 0, "device");
      tablet.addValue(0, 1, 1L);

      builder.bufferTableModelTablet("database", tablet);
      fallbackFiles.addAll(builder.convertTabletToTsFileWithDBInfo());

      Assert.assertEquals(1, fallbackFiles.size());
      Assert.assertEquals("database", fallbackFiles.get(0).left);
      Assert.assertTrue(fallbackFiles.get(0).right.isFile());
      Assert.assertTrue(fallbackFiles.get(0).right.length() > 0);
    } finally {
      builder.close();
      fallbackFiles.forEach(file -> FileUtils.deleteQuietly(file.right));
    }
  }
}
