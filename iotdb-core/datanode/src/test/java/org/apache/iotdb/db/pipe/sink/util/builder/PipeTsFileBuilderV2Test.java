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
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
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
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

public class PipeTsFileBuilderV2Test {

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

  @Test
  public void testFallbackDeletesPrimaryTableModelFiles() throws Exception {
    final AtomicInteger fileIndex = new AtomicInteger();
    final List<File> primaryFiles = new ArrayList<>();
    final PipeTableModelTsFileBuilderV2 builder =
        new PipeTableModelTsFileBuilderV2(new AtomicLong(2), new AtomicLong(0)) {
          @Override
          protected File createFile() throws IOException {
            final int index = fileIndex.incrementAndGet();
            final File file =
                new File(temporaryPipeReceiverDir, "primary-table-" + index + ".tsfile");
            primaryFiles.add(file);
            if (index == 2 && !file.mkdir()) {
              throw new IOException("Failed to create invalid primary file path");
            }
            return file;
          }
        };
    final List<Pair<String, File>> fallbackFiles = new ArrayList<>();
    try {
      builder.bufferTableModelTablet("database1", createTableModelTablet("table1"));
      builder.bufferTableModelTablet("database2", createTableModelTablet("table2"));
      fallbackFiles.addAll(builder.convertTabletToTsFileWithDBInfo());

      Assert.assertEquals(2, fallbackFiles.size());
      Assert.assertEquals(2, primaryFiles.size());
      primaryFiles.forEach(file -> Assert.assertFalse(file.exists()));
    } finally {
      builder.close();
      fallbackFiles.forEach(file -> FileUtils.deleteQuietly(file.right));
      primaryFiles.forEach(FileUtils::deleteQuietly);
    }
  }

  @Test
  public void testClassicTableModelBuilderDeletesEarlierDatabaseFilesOnFailure() throws Exception {
    final AtomicInteger fileIndex = new AtomicInteger();
    final List<File> createdFiles = new ArrayList<>();
    final PipeTableModelTsFileBuilder builder =
        new PipeTableModelTsFileBuilder(new AtomicLong(5), new AtomicLong(0)) {
          @Override
          protected File createFile() throws IOException {
            final int index = fileIndex.incrementAndGet();
            final File file =
                new File(temporaryPipeReceiverDir, "classic-table-" + index + ".tsfile");
            createdFiles.add(file);
            if (index == 2) {
              Assert.assertTrue(createdFiles.get(0).isFile());
              if (!file.mkdir()) {
                throw new IOException("Failed to create invalid classic file path");
              }
            }
            return file;
          }
        };
    try {
      builder.bufferTableModelTablet("database1", createTableModelTablet("table1"));
      builder.bufferTableModelTablet("database2", createTableModelTablet("table2"));

      try {
        builder.convertTabletToTsFileWithDBInfo();
        Assert.fail("Expected the second database conversion to fail");
      } catch (final Exception expected) {
        // expected
      }

      Assert.assertEquals(2, createdFiles.size());
      createdFiles.forEach(file -> Assert.assertFalse(file.exists()));
    } finally {
      builder.close();
      createdFiles.forEach(FileUtils::deleteQuietly);
    }
  }

  @Test
  public void testTreeModelBuilderPreservesColumnsAfterNullSchema() throws Exception {
    final PipeTreeModelTsFileBuilderV2 builder =
        new PipeTreeModelTsFileBuilderV2(new AtomicLong(3), new AtomicLong(0));
    final List<Pair<String, File>> files = new ArrayList<>();
    try {
      final List<IMeasurementSchema> schemas =
          Arrays.asList(
              new MeasurementSchema("s0", TSDataType.INT64, TSEncoding.PLAIN),
              new MeasurementSchema("s1", TSDataType.INT64, TSEncoding.PLAIN),
              new MeasurementSchema("s2", TSDataType.INT64, TSEncoding.PLAIN));
      final Tablet tablet =
          new Tablet(
              "root.database.device",
              schemas,
              new long[] {1L},
              new Object[] {new long[] {1L}, new long[] {2L}, new long[] {3L}},
              null,
              1);
      tablet.getSchemas().set(1, null);

      builder.bufferTreeModelTablet(tablet, false);
      files.addAll(builder.convertTabletToTsFileWithDBInfo());

      Assert.assertEquals(1, files.size());
      try (final TsFileSequenceReader reader =
          new TsFileSequenceReader(files.get(0).right.getAbsolutePath())) {
        final IDeviceID deviceID = IDeviceID.Factory.DEFAULT_FACTORY.create("root.database.device");
        Assert.assertNotNull(reader.readTimeseriesMetadata(deviceID, "s0", true));
        Assert.assertNotNull(reader.readTimeseriesMetadata(deviceID, "s2", true));
      }
    } finally {
      builder.close();
      files.forEach(file -> FileUtils.deleteQuietly(file.right));
    }
  }

  @Test
  public void testFallbackDeletesPrimaryTreeModelFile() throws Exception {
    final File primaryFile = new File(temporaryPipeReceiverDir, "primary-tree.tsfile");
    final PipeTreeModelTsFileBuilderV2 builder =
        new PipeTreeModelTsFileBuilderV2(new AtomicLong(4), new AtomicLong(0)) {
          @Override
          protected File createFile() throws IOException {
            if (!primaryFile.mkdir()) {
              throw new IOException("Failed to create invalid primary file path");
            }
            return primaryFile;
          }
        };
    final List<Pair<String, File>> fallbackFiles = new ArrayList<>();
    try {
      final Tablet tablet =
          new Tablet(
              "root.database.device",
              Arrays.<IMeasurementSchema>asList(
                  new MeasurementSchema("s0", TSDataType.INT64, TSEncoding.PLAIN)),
              1);
      tablet.addTimestamp(0, 1L);
      tablet.addValue(0, 0, 1L);

      builder.bufferTreeModelTablet(tablet, false);
      fallbackFiles.addAll(builder.convertTabletToTsFileWithDBInfo());

      Assert.assertEquals(1, fallbackFiles.size());
      Assert.assertFalse(primaryFile.exists());
    } finally {
      builder.close();
      fallbackFiles.forEach(file -> FileUtils.deleteQuietly(file.right));
      FileUtils.deleteQuietly(primaryFile);
    }
  }

  private static Tablet createTableModelTablet(final String tableName) {
    final Tablet tablet =
        new Tablet(
            tableName,
            Arrays.asList("tag", "field"),
            Arrays.asList(TSDataType.STRING, TSDataType.INT64),
            Arrays.asList(ColumnCategory.TAG, ColumnCategory.FIELD),
            1);
    tablet.addTimestamp(0, 1L);
    tablet.addValue(0, 0, "device");
    tablet.addValue(0, 1, 1L);
    return tablet;
  }
}
