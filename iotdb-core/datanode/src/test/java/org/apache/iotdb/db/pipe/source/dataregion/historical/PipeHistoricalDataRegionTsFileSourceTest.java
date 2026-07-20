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

package org.apache.iotdb.db.pipe.source.dataregion.historical;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.HybridProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.IoTProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PrefixPipePattern;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;

import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;

public class PipeHistoricalDataRegionTsFileSourceTest {

  @Test
  public void testMayTsFileContainUnprocessedDataUsesEqualOrAfterCoverage() throws Exception {
    final File tempDir = Files.createTempDirectory("pipeHistoricalProgressCoverage").toFile();

    try {
      assertMayTsFileContainUnprocessedData(
          tempDir,
          "superset.tsfile",
          hybridProgressIndex(
              new IoTProgressIndex(ImmutableMap.of(1, 100L, 2, 200L)),
              new RecoverProgressIndex(-1, new SimpleProgressIndex(0, 10))),
          hybridProgressIndex(
              new IoTProgressIndex(1, 100L),
              new RecoverProgressIndex(-1, new SimpleProgressIndex(0, 9))),
          false);

      assertMayTsFileContainUnprocessedData(
          tempDir,
          "missing-dimension.tsfile",
          hybridProgressIndex(new IoTProgressIndex(1, 100L)),
          hybridProgressIndex(
              new IoTProgressIndex(1, 90L),
              new RecoverProgressIndex(-1, new SimpleProgressIndex(0, 10))),
          true);
    } finally {
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  public void testTsFileResourceCoveredByPattern() throws Exception {
    final File tempDir = Files.createTempDirectory("pipeHistoricalPatternCoverage").toFile();

    try {
      final PipeHistoricalDataRegionTsFileSource source =
          new PipeHistoricalDataRegionTsFileSource();
      final Method method =
          PipeHistoricalDataRegionTsFileSource.class.getDeclaredMethod(
              "isTsFileResourceCoveredByPattern", TsFileResource.class);
      method.setAccessible(true);

      final TsFileResource resource =
          createClosedTsFileResourceWithDevices(
              tempDir, "covered-pattern.tsfile", "root.sg.d1", "root.sg.d2");

      setPrivateField(source, "pipePattern", new PrefixPipePattern("root.sg"));
      Assert.assertTrue((Boolean) method.invoke(source, resource));

      setPrivateField(source, "pipePattern", new PrefixPipePattern("root.sg.d1"));
      Assert.assertFalse((Boolean) method.invoke(source, resource));
      Assert.assertFalse(
          (Boolean)
              method.invoke(
                  source,
                  createClosedTsFileResource(
                      tempDir, "empty-device.tsfile", new SimpleProgressIndex(0, 1))));
    } finally {
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  private static void assertMayTsFileContainUnprocessedData(
      final File tempDir,
      final String fileName,
      final ProgressIndex startIndex,
      final ProgressIndex resourceProgressIndex,
      final boolean expected)
      throws Exception {
    Assert.assertEquals(!expected, startIndex.isEqualOrAfter(resourceProgressIndex));

    final PipeHistoricalDataRegionTsFileSource source = new PipeHistoricalDataRegionTsFileSource();
    setPrivateField(source, "pipeName", "pipe");
    setPrivateField(source, "dataRegionId", 1);
    setPrivateField(source, "startIndex", startIndex);

    final Method method =
        PipeHistoricalDataRegionTsFileSource.class.getDeclaredMethod(
            "mayTsFileContainUnprocessedData", TsFileResource.class);
    method.setAccessible(true);
    Assert.assertEquals(
        expected,
        method.invoke(
            source, createClosedTsFileResource(tempDir, fileName, resourceProgressIndex)));
  }

  private static TsFileResource createClosedTsFileResource(
      final File tempDir, final String fileName, final ProgressIndex progressIndex)
      throws Exception {
    final File file = new File(tempDir, fileName);
    Assert.assertTrue(file.createNewFile());

    final TsFileResource resource = new TsFileResource(file);
    resource.setStatusForTest(TsFileResourceStatus.NORMAL);
    resource.updateProgressIndex(progressIndex);
    return resource;
  }

  private static TsFileResource createClosedTsFileResourceWithDevices(
      final File tempDir, final String fileName, final String... devices) throws Exception {
    final TsFileResource resource =
        createClosedTsFileResource(tempDir, fileName, new SimpleProgressIndex(0, 1));
    for (final String device : devices) {
      final PlainDeviceID deviceID = new PlainDeviceID(device);
      resource.updateStartTime(deviceID, 0);
      resource.updateEndTime(deviceID, 1);
    }
    return resource;
  }

  private static ProgressIndex hybridProgressIndex(
      final ProgressIndex firstProgressIndex, final ProgressIndex... progressIndexes) {
    ProgressIndex result = new HybridProgressIndex(firstProgressIndex);
    for (final ProgressIndex progressIndex : progressIndexes) {
      result = result.updateToMinimumEqualOrIsAfterProgressIndex(progressIndex);
    }
    return result;
  }

  private static void setPrivateField(
      final PipeHistoricalDataRegionTsFileSource source, final String fieldName, final Object value)
      throws ReflectiveOperationException {
    final Field field = PipeHistoricalDataRegionTsFileSource.class.getDeclaredField(fieldName);
    field.setAccessible(true);
    field.set(source, value);
  }
}
