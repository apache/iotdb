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
import org.apache.iotdb.commons.consensus.index.impl.MinimumProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.RecoverProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.TimePartitionProgressIndex;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.pipe.datastructure.pattern.PrefixPipePattern;
import org.apache.iotdb.commons.pipe.event.ProgressReportEvent;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

import com.google.common.collect.ImmutableMap;
import org.apache.tsfile.file.metadata.PlainDeviceID;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.file.Files;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class PipeHistoricalDataRegionTsFileSourceTest {

  @Test
  public void testGlobalTimeRangeRespectsHistoryEnable() throws Exception {
    final Map<String, String> attributes = new HashMap<>();
    attributes.put(PipeSourceConstant.SOURCE_START_TIME_KEY, "1000");
    attributes.put(PipeSourceConstant.SOURCE_HISTORY_ENABLE_KEY, Boolean.FALSE.toString());

    final PipeHistoricalDataRegionTsFileSource realtimeOnlySource =
        new PipeHistoricalDataRegionTsFileSource();
    realtimeOnlySource.validate(
        new PipeParameterValidator(new PipeParameters(new HashMap<>(attributes))));

    Assert.assertFalse((Boolean) getPrivateField(realtimeOnlySource, "isHistoricalSourceEnabled"));
    Assert.assertEquals(
        1000L,
        ((Long) getPrivateField(realtimeOnlySource, "historicalDataExtractionStartTime"))
            .longValue());

    final PipeHistoricalDataRegionTsFileSource defaultSource =
        new PipeHistoricalDataRegionTsFileSource();
    attributes.remove(PipeSourceConstant.SOURCE_HISTORY_ENABLE_KEY);
    defaultSource.validate(
        new PipeParameterValidator(new PipeParameters(new HashMap<>(attributes))));

    Assert.assertTrue((Boolean) getPrivateField(defaultSource, "isHistoricalSourceEnabled"));

    final PipeHistoricalDataRegionTsFileSource restartedSource =
        new PipeHistoricalDataRegionTsFileSource();
    attributes.put(PipeSourceConstant.SOURCE_HISTORY_ENABLE_KEY, Boolean.FALSE.toString());
    attributes.put(SystemConstant.RESTART_OR_NEWLY_ADDED_KEY, Boolean.TRUE.toString());
    restartedSource.validate(
        new PipeParameterValidator(new PipeParameters(new HashMap<>(attributes))));

    Assert.assertTrue((Boolean) getPrivateField(restartedSource, "isHistoricalSourceEnabled"));
  }

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
  public void testHistoricalTsFileQueryPriorityOrderDefaultsToTrue() {
    final PipeHistoricalDataRegionTsFileSource source = new PipeHistoricalDataRegionTsFileSource();

    source.validate(new PipeParameterValidator(new PipeParameters(new HashMap<>())));

    Assert.assertTrue(
        (Boolean) getPrivateField(source, "shouldOrderHistoricalTsFileByQueryPriority"));
  }

  @Test
  public void testHistoricalTsFileQueryPriorityOrderMatchesQueryCoverage() throws Exception {
    final PipeHistoricalDataRegionTsFileSource source = new PipeHistoricalDataRegionTsFileSource();
    final File tempDir = Files.createTempDirectory("pipeHistoricalTsFileOrder").toFile();

    try {
      final TsFileResource seqLowerVersionNewerFileTimestamp =
          createTsFileResource(tempDir, "300-1-0-0.tsfile");
      seqLowerVersionNewerFileTimestamp.setSeq(true);
      final TsFileResource seqSameVersionOlderFileTimestamp =
          createTsFileResource(tempDir, "100-2-0-0.tsfile");
      seqSameVersionOlderFileTimestamp.setSeq(true);
      final TsFileResource seqSameVersionNewerFileTimestamp =
          createTsFileResource(tempDir, "200-2-0-0.tsfile");
      seqSameVersionNewerFileTimestamp.setSeq(true);
      final TsFileResource seqHigherVersionOlderFileTimestamp =
          createTsFileResource(tempDir, "50-3-0-0.tsfile");
      seqHigherVersionOlderFileTimestamp.setSeq(true);
      final TsFileResource unseqLowerVersionOldestFileTimestamp =
          createTsFileResource(tempDir, "1-1-0-0.tsfile");
      unseqLowerVersionOldestFileTimestamp.setSeq(false);

      setPrivateField(source, "shouldOrderHistoricalTsFileByQueryPriority", true);
      setPrivateField(source, "shouldExtractInsertion", true);
      setPrivateField(source, "startIndex", MinimumProgressIndex.INSTANCE);

      final List<TsFileResource> resources =
          new ArrayList<>(
              Arrays.asList(
                  unseqLowerVersionOldestFileTimestamp,
                  seqHigherVersionOlderFileTimestamp,
                  seqSameVersionNewerFileTimestamp,
                  seqSameVersionOlderFileTimestamp,
                  seqLowerVersionNewerFileTimestamp));
      sortExtractedResources(source, resources);

      Assert.assertEquals(
          Arrays.asList(
              seqLowerVersionNewerFileTimestamp,
              seqSameVersionOlderFileTimestamp,
              seqSameVersionNewerFileTimestamp,
              seqHigherVersionOlderFileTimestamp,
              unseqLowerVersionOldestFileTimestamp),
          resources);
    } finally {
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  public void testHistoricalTsFileQueryPriorityOrderCanBeDisabled() throws Exception {
    final PipeHistoricalDataRegionTsFileSource source = new PipeHistoricalDataRegionTsFileSource();
    final PipeParameters parameters =
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put(
                    PipeSourceConstant.SOURCE_HISTORY_TSFILE_ORDER_BY_QUERY_PRIORITY_KEY,
                    Boolean.FALSE.toString());
              }
            });
    final File tempDir = Files.createTempDirectory("pipeHistoricalTsFileProgressOrder").toFile();

    try {
      source.validate(new PipeParameterValidator(parameters));
      final TsFileResource earlierProgressIndex = createTsFileResource(tempDir, "300-1-0-0.tsfile");
      earlierProgressIndex.updateProgressIndex(new SimpleProgressIndex(0, 1));
      final TsFileResource laterProgressIndex = createTsFileResource(tempDir, "100-1-0-0.tsfile");
      laterProgressIndex.updateProgressIndex(new SimpleProgressIndex(0, 2));

      setPrivateField(source, "shouldExtractInsertion", true);
      setPrivateField(source, "startIndex", MinimumProgressIndex.INSTANCE);

      final List<TsFileResource> resources =
          new ArrayList<>(Arrays.asList(laterProgressIndex, earlierProgressIndex));
      sortExtractedResources(source, resources);

      Assert.assertFalse(
          (Boolean) getPrivateField(source, "shouldOrderHistoricalTsFileByQueryPriority"));
      Assert.assertEquals(Arrays.asList(earlierProgressIndex, laterProgressIndex), resources);
    } finally {
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  public void testHistoricalTsFileQueryPriorityOrderCanBeDisabledByExtractorKey() {
    final PipeHistoricalDataRegionTsFileSource source = new PipeHistoricalDataRegionTsFileSource();
    final PipeParameters parameters =
        new PipeParameters(
            new HashMap<String, String>() {
              {
                put(
                    PipeSourceConstant.EXTRACTOR_HISTORY_TSFILE_ORDER_BY_QUERY_PRIORITY_KEY,
                    Boolean.FALSE.toString());
              }
            });

    source.validate(new PipeParameterValidator(parameters));

    Assert.assertFalse(
        (Boolean) getPrivateField(source, "shouldOrderHistoricalTsFileByQueryPriority"));
  }

  @Test
  @SuppressWarnings("unchecked")
  public void testQueryPriorityOrderProgressOnlyCoversSelectedResources() throws Exception {
    final PipeHistoricalDataRegionTsFileSource source = new PipeHistoricalDataRegionTsFileSource();
    final File tempDir = Files.createTempDirectory("pipeHistoricalTsFileSelectedProgress").toFile();

    try {
      final TsFileResource selectedResource = createTsFileResource(tempDir, "100-1-0-0.tsfile");
      selectedResource.updateProgressIndex(new SimpleProgressIndex(0, 1));
      final TsFileResource filteredResource = createTsFileResource(tempDir, "200-1-0-0.tsfile");
      filteredResource.updateProgressIndex(new SimpleProgressIndex(0, 100));

      ((Set<TsFileResource>) getPrivateField(source, "filteredTsFileResources"))
          .add(selectedResource);

      final List<TsFileResource> resources =
          new ArrayList<>(Arrays.asList(filteredResource, selectedResource));
      prepareResourcesForHistoricalTsFileQueryPriorityOrder(source, resources);

      Assert.assertEquals(Arrays.asList(selectedResource), resources);
      Assert.assertEquals(
          new SimpleProgressIndex(0, 1), getPrivateField(source, "maxHistoricalProgressIndex"));
      Assert.assertTrue(
          (Boolean) getPrivateField(source, "shouldReportMaxHistoricalProgressIndex"));
    } finally {
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  public void testQueryPriorityOrderPreparesIncrementalSafeProgressReports() throws Exception {
    final PipeHistoricalDataRegionTsFileSource source = new PipeHistoricalDataRegionTsFileSource();
    final File tempDir =
        Files.createTempDirectory("pipeHistoricalTsFileIncrementalProgress").toFile();

    try {
      final TsFileResource progress1 = createTsFileResource(tempDir, "1.tsfile");
      progress1.updateProgressIndex(new SimpleProgressIndex(0, 1));
      final TsFileResource progress2 = createTsFileResource(tempDir, "2.tsfile");
      progress2.updateProgressIndex(new SimpleProgressIndex(0, 2));
      final TsFileResource progress4 = createTsFileResource(tempDir, "4.tsfile");
      progress4.updateProgressIndex(new SimpleProgressIndex(0, 4));
      final TsFileResource progress3 = createTsFileResource(tempDir, "3.tsfile");
      progress3.updateProgressIndex(new SimpleProgressIndex(0, 3));
      final TsFileResource progress5 = createTsFileResource(tempDir, "5.tsfile");
      progress5.updateProgressIndex(new SimpleProgressIndex(0, 5));

      final List<TsFileResource> resources =
          new ArrayList<>(Arrays.asList(progress1, progress2, progress4, progress3, progress5));
      prepareProgressReportResourcesForHistoricalTsFileQueryPriorityOrder(source, resources);

      Assert.assertEquals(
          new HashSet<>(Arrays.asList(progress1, progress2, progress3, progress5)),
          getPrivateField(source, "historicalProgressReportResources"));
    } finally {
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  public void testQueryPriorityOrderPreparesSafeProgressReportsByTimePartition() throws Exception {
    final PipeHistoricalDataRegionTsFileSource source = new PipeHistoricalDataRegionTsFileSource();
    final File tempDir =
        Files.createTempDirectory("pipeHistoricalTsFilePartitionProgress").toFile();

    try {
      final TsFileResource partition0Progress100 =
          createTsFileResource(tempDir, 0L, "100-1-0-0.tsfile");
      partition0Progress100.updateProgressIndex(new SimpleProgressIndex(0, 100));
      final TsFileResource partition1Progress20 =
          createTsFileResource(tempDir, 1L, "20-1-0-0.tsfile");
      partition1Progress20.updateProgressIndex(new SimpleProgressIndex(0, 20));
      final List<TsFileResource> resources =
          new ArrayList<>(Arrays.asList(partition0Progress100, partition1Progress20));
      prepareProgressReportResourcesForHistoricalTsFileQueryPriorityOrder(source, resources);

      Assert.assertEquals(
          new HashSet<>(Arrays.asList(partition0Progress100, partition1Progress20)),
          getPrivateField(source, "historicalProgressReportResources"));
    } finally {
      FileUtils.deleteFileOrDirectory(tempDir);
    }
  }

  @Test
  public void testQueryPriorityOrderReportsProgressAfterAllHistoricalResources() {
    final PipeHistoricalDataRegionTsFileSource source = new PipeHistoricalDataRegionTsFileSource();
    final ProgressIndex expectedProgressIndex = new SimpleProgressIndex(0, 10);

    setPrivateField(source, "hasBeenStarted", true);
    setPrivateField(source, "pipeName", "pipe");
    setPrivateField(source, "creationTime", 1L);
    setPrivateField(source, "dataRegionId", 1);
    setPrivateField(source, "pipeTaskMeta", new PipeTaskMeta(MinimumProgressIndex.INSTANCE, 1));
    setPrivateField(source, "pendingQueue", new ArrayDeque<TsFileResource>());
    setPrivateField(source, "maxHistoricalProgressIndex", expectedProgressIndex);
    setPrivateField(source, "shouldReportMaxHistoricalProgressIndex", true);

    final Event event = source.supply();

    Assert.assertTrue(event instanceof ProgressReportEvent);
    Assert.assertEquals(expectedProgressIndex, ((ProgressReportEvent) event).getProgressIndex());
    Assert.assertFalse((Boolean) getPrivateField(source, "shouldReportMaxHistoricalProgressIndex"));
  }

  @Test
  public void testMayTsFileContainUnprocessedDataUsesTimePartitionProgressCoverage()
      throws Exception {
    final File tempDir = Files.createTempDirectory("pipeHistoricalPartitionCoverage").toFile();

    try {
      final ProgressIndex startIndex =
          new TimePartitionProgressIndex(0L, new SimpleProgressIndex(0, 100));
      assertMayTsFileContainUnprocessedData(
          tempDir,
          0L,
          "partition-covered.tsfile",
          startIndex,
          new SimpleProgressIndex(0, 50),
          false);
      assertMayTsFileContainUnprocessedData(
          tempDir,
          1L,
          "partition-uncovered.tsfile",
          startIndex,
          new SimpleProgressIndex(0, 50),
          true);

      final ProgressIndex hybridStartIndex =
          hybridProgressIndex(
              startIndex, new RecoverProgressIndex(-1, new SimpleProgressIndex(0, 1)));
      assertMayTsFileContainUnprocessedData(
          tempDir,
          0L,
          "hybrid-partition-covered.tsfile",
          hybridStartIndex,
          new SimpleProgressIndex(0, 80),
          false);
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
    assertMayTsFileContainUnprocessedData(
        startIndex, createClosedTsFileResource(tempDir, fileName, resourceProgressIndex), expected);
  }

  private static void assertMayTsFileContainUnprocessedData(
      final File tempDir,
      final long timePartitionId,
      final String fileName,
      final ProgressIndex startIndex,
      final ProgressIndex resourceProgressIndex,
      final boolean expected)
      throws Exception {
    assertMayTsFileContainUnprocessedData(
        startIndex,
        createClosedTsFileResource(tempDir, timePartitionId, fileName, resourceProgressIndex),
        expected);
  }

  private static void assertMayTsFileContainUnprocessedData(
      final ProgressIndex startIndex, final TsFileResource resource, final boolean expected)
      throws Exception {
    final PipeHistoricalDataRegionTsFileSource source = new PipeHistoricalDataRegionTsFileSource();
    setPrivateField(source, "pipeName", "pipe");
    setPrivateField(source, "dataRegionId", 1);
    setPrivateField(source, "startIndex", startIndex);

    final Method method =
        PipeHistoricalDataRegionTsFileSource.class.getDeclaredMethod(
            "mayTsFileContainUnprocessedData", TsFileResource.class);
    method.setAccessible(true);
    Assert.assertEquals(expected, method.invoke(source, resource));
  }

  private static TsFileResource createTsFileResource(final File tempDir, final String fileName)
      throws IOException {
    final File file = new File(tempDir, fileName);
    Assert.assertTrue(file.createNewFile());
    return new TsFileResource(file);
  }

  private static TsFileResource createTsFileResource(
      final File tempDir, final long timePartitionId, final String fileName) throws IOException {
    final File regionDir = new File(tempDir, "1");
    final File partitionDir = new File(regionDir, String.valueOf(timePartitionId));
    Assert.assertTrue(partitionDir.exists() || partitionDir.mkdirs());
    return createTsFileResource(partitionDir, fileName);
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

  private static TsFileResource createClosedTsFileResource(
      final File tempDir,
      final long timePartitionId,
      final String fileName,
      final ProgressIndex progressIndex)
      throws IOException {
    final TsFileResource resource = createTsFileResource(tempDir, timePartitionId, fileName);
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

  private static void sortExtractedResources(
      final PipeHistoricalDataRegionTsFileSource source, final List<TsFileResource> resources)
      throws ReflectiveOperationException {
    final Method method =
        PipeHistoricalDataRegionTsFileSource.class.getDeclaredMethod(
            "sortExtractedResources", List.class);
    method.setAccessible(true);
    method.invoke(source, resources);
  }

  private static void prepareResourcesForHistoricalTsFileQueryPriorityOrder(
      final PipeHistoricalDataRegionTsFileSource source, final List<TsFileResource> resources)
      throws ReflectiveOperationException {
    final Method method =
        PipeHistoricalDataRegionTsFileSource.class.getDeclaredMethod(
            "prepareResourcesForHistoricalTsFileQueryPriorityOrder", List.class);
    method.setAccessible(true);
    method.invoke(source, resources);
  }

  private static void prepareProgressReportResourcesForHistoricalTsFileQueryPriorityOrder(
      final PipeHistoricalDataRegionTsFileSource source, final List<TsFileResource> resources)
      throws ReflectiveOperationException {
    final Method method =
        PipeHistoricalDataRegionTsFileSource.class.getDeclaredMethod(
            "prepareProgressReportResourcesForHistoricalTsFileQueryPriorityOrder", List.class);
    method.setAccessible(true);
    method.invoke(source, resources);
  }

  private static Object getPrivateField(
      final PipeHistoricalDataRegionTsFileSource source, final String fieldName) {
    try {
      final Field field = PipeHistoricalDataRegionTsFileSource.class.getDeclaredField(fieldName);
      field.setAccessible(true);
      return field.get(source);
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }

  private static void setPrivateField(
      final PipeHistoricalDataRegionTsFileSource source,
      final String fieldName,
      final Object value) {
    try {
      final Field field = PipeHistoricalDataRegionTsFileSource.class.getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(source, value);
    } catch (final ReflectiveOperationException e) {
      throw new AssertionError(e);
    }
  }
}
