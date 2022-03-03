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

package org.apache.iotdb.db.pipe.external;

import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.newsync.datasource.PipeOpManager;
import org.apache.iotdb.pipe.external.api.IExternalPipeSinkWriter;
import org.apache.iotdb.pipe.external.api.IExternalPipeSinkWriterFactory;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ExternalPipeTest {

  private static final int NUM_OF_STORAGE_GROUPS = 2;
  private static final int NUM_OF_DEVICES_PER_STORAGE_GROUP = 2;
  private static final int NUM_OF_MEASUREMENTS_PER_DEVICE = 2;
  private static final List<MeasurementPath> MEASUREMENT_PATHS;
  // private static final List<ExternalPipeTimeseriesGroup> EXTERNAL_PIPE_TIMESERIES_GROUPS;
  Map<String, String> sinkParams = new HashMap<>();
  Set<String> sgSet = new HashSet<>();
  private static final Random RANDOM = new Random();

  static {
    MEASUREMENT_PATHS = new ArrayList<>();
    for (int i = 0; i < NUM_OF_STORAGE_GROUPS; i++) {
      for (int j = 0; j < NUM_OF_DEVICES_PER_STORAGE_GROUP; j++) {
        for (int k = 0; k < NUM_OF_MEASUREMENTS_PER_DEVICE; k++) {
          String[] path = new String[] {"root", "sg" + i, "d" + j, "s" + k};
          MEASUREMENT_PATHS.add(
              new MeasurementPath(
                  new PartialPath(path),
                  new MeasurementSchema(path[path.length - 1], TSDataType.DOUBLE)));
        }
      }
    }
    //    EXTERNAL_PIPE_TIMESERIES_GROUPS = new ArrayList<>();
    //    int numOfMeasurementsPerStorageGroup =
    //        NUM_OF_DEVICES_PER_STORAGE_GROUP * NUM_OF_MEASUREMENTS_PER_DEVICE;
    //    for (int i = 0; i < NUM_OF_STORAGE_GROUPS; i++) {
    //      EXTERNAL_PIPE_TIMESERIES_GROUPS.add(
    //          new ExternalPipeTimeseriesGroup(
    //              MEASUREMENT_PATHS.subList(
    //                  i * numOfMeasurementsPerStorageGroup,
    //                  (i + 1) * numOfMeasurementsPerStorageGroup)));
    //    }
  }

  private PipeOpManager MockPipeOpManager;
  private IExternalPipeSinkWriterFactory mockExternalPipeSinkWriterFactory;
  private IExternalPipeSinkWriter mockExternalPipeSinkWriter;
  private Map<ExternalPipeTimeseriesGroup, Long> groupToTimestamp = new HashMap<>();

  @Before
  public void before() throws IOException {
    // Reset groupToTimestamp
    //    for (ExternalPipeTimeseriesGroup group : EXTERNAL_PIPE_TIMESERIES_GROUPS) {
    //      groupToTimestamp.put(group, 0L);
    //    }
    // Initialize a mock ExternalPipeSource
    MockPipeOpManager = mock(PipeOpManager.class);
    when(MockPipeOpManager.getSgSet())
        .thenAnswer(
            args -> {
              Set<String> sgSet = new HashSet<>();
              sgSet.add("sg1");
              return sgSet;
            });
    //    when(MockPipeOpManager.getFirstAvailableIndex(Mockito.any())).thenReturn(0L);
    //    when(MockPipeOpManager.getLastCommittedIndex(Mockito.any())).thenReturn(-1L);
    //    when(MockPipeOpManager.getOperations(Mockito.any(), Mockito.anyLong(), Mockito.anyInt()))
    //        .thenAnswer(
    //            args -> {
    //              ExternalPipeTimeseriesGroup group = args.getArgument(0);
    //              long index = args.getArgument(1);
    //              int length = args.getArgument(2);
    //              int count = 0;
    //              List<Pair<MeasurementPath, List<TimeValuePair>>> pathToTimeValuePairs =
    //                  new LinkedList<>();
    //              while (count < length) {
    //                for (MeasurementPath measurementPath : group.getMeasurementPaths()) {
    //                  List<TimeValuePair> tvList = new LinkedList<>();
    //                  tvList.add(
    //                      new TimeValuePair(
    //                          groupToTimestamp.get(group), new TsFloat(RANDOM.nextFloat())));
    //                  pathToTimeValuePairs.add(new Pair<>(measurementPath, tvList));
    //                  groupToTimestamp.merge(group, 1L, Long::sum);
    //                  count += 1;
    //                  if (count >= length) {
    //                    break;
    //                  }
    //                }
    //              }
    //              return Collections.singletonList(
    //                  new InsertOperation("", index, index + length - 1, pathToTimeValuePairs));
    //            });

    // Initialize a mock IExternalPipeSinkWriterFactory
    mockExternalPipeSinkWriterFactory = mock(IExternalPipeSinkWriterFactory.class);
    mockExternalPipeSinkWriter = mock(IExternalPipeSinkWriter.class);
    when(mockExternalPipeSinkWriterFactory.get()).thenReturn(mockExternalPipeSinkWriter);
  }

  /** Test if the data transmission works. */
  @Test(timeout = 10_000L)
  public void testSingleThreadedDataTransmission() throws InterruptedException, IOException {
    // Run the pipe for a second.
    String extPipeName = "TestPipe";
    sinkParams.clear();
    sinkParams.put("thread_num", "1");
    ExternalPipe externalPipe = new ExternalPipe(extPipeName, sinkParams, MockPipeOpManager);
    externalPipe.setPipeSinkWriterFactory(mockExternalPipeSinkWriterFactory);

    externalPipe.start();
    Thread.sleep(100);
    externalPipe.stop();

    // Verify that the external pipe interacts with the external pipe source in the expected way.
    verify(MockPipeOpManager, times(2)).getSgSet();
    //    verify(MockPipeOpManager, times(1)).getLastCommittedIndex(group);
    //    verify(MockPipeOpManager, times(1)).getFirstAvailableIndex(group);
    //
    //    // Verify that the external pipe called getOperations with expected indices. The index
    // should
    //    // start with 0, and increase by the batch size for each invocation.
    //    Iterator<Invocation> MockPipeOpManagerInvocationIterator =
    //        mockingDetails(MockPipeOpManager).getInvocations().iterator();
    //    int numOfInvocationsOfGetOperations = 0;
    //    while (MockPipeOpManagerInvocationIterator.hasNext()) {
    //      Invocation invocation = MockPipeOpManagerInvocationIterator.next();
    //      if (invocation.getMethod().getName().equals("getOperations")) {
    //        numOfInvocationsOfGetOperations += 1;
    //      }
    //    }
    //    for (int i = 0; i < numOfInvocationsOfGetOperations; i++) {
    //      verify(MockPipeOpManager, times(1))
    //          .getOperations(
    //              group,
    //              (long) i * externalPipeConfiguration.getOperationBatchSize(),
    //              externalPipeConfiguration.getOperationBatchSize());
    //    }
    //
    //    // Verify that the insert methods of writer get called as expected.
    //    verify(
    //            mockExternalPipeSinkWriter,
    //            times(
    //                numOfInvocationsOfGetOperations
    //                    * externalPipeConfiguration.getOperationBatchSize()))
    //        .insertFloat(any(), anyLong(), anyFloat());
  }
  //
  //  /** Test if the data transmission works. */
  //  @Test(timeout = 10_000L)
  //  public void testMultiThreadedDataTransmission() throws InterruptedException, IOException {
  //    // Run the pipe for a second.
  //    ExternalPipeConfiguration externalPipeConfiguration =
  //        new ExternalPipeConfiguration.Builder("TestPipe").numOfThreads(2).build();
  //    ExternalPipe externalPipe =
  //        new ExternalPipe(
  //            "Test",
  //            MockPipeOpManager,
  //            mockExternalPipeSinkWriterFactory,
  //            externalPipeConfiguration);
  //    externalPipe.start();
  //    Thread.sleep(100);
  //    externalPipe.stop();
  //
  //    // Verify that the external pipe interacts with the external pipe source in the expected
  // way.
  //    ExternalPipeTimeseriesGroup group1 = EXTERNAL_PIPE_TIMESERIES_GROUPS.get(0);
  //    ExternalPipeTimeseriesGroup group2 = EXTERNAL_PIPE_TIMESERIES_GROUPS.get(0);
  //    verify(MockPipeOpManager, times(1)).getSgSet(2);
  //    verify(MockPipeOpManager, times(1)).getLastCommittedIndex(group1);
  //    verify(MockPipeOpManager, times(1)).getFirstAvailableIndex(group1);
  //    verify(MockPipeOpManager, times(1)).getLastCommittedIndex(group2);
  //    verify(MockPipeOpManager, times(1)).getFirstAvailableIndex(group2);
  //
  //    // Verify that the external pipe called getOperations with expected indices. The index
  // should
  //    // start with 0, and increase by the batch size for each invocation.
  //    Iterator<Invocation> MockPipeOpManagerInvocationIterator =
  //        mockingDetails(MockPipeOpManager).getInvocations().iterator();
  //    Map<ExternalPipeTimeseriesGroup, Integer> groupToNumOfInvocationsOfGetOperations =
  //        new HashMap<>();
  //    while (MockPipeOpManagerInvocationIterator.hasNext()) {
  //      Invocation invocation = MockPipeOpManagerInvocationIterator.next();
  //      if ("getOperations".equals(invocation.getMethod().getName())) {
  //        ExternalPipeTimeseriesGroup group = invocation.getArgument(0);
  //        if (!groupToNumOfInvocationsOfGetOperations.containsKey(group)) {
  //          groupToNumOfInvocationsOfGetOperations.put(group, 0);
  //        }
  //        int numOfInvocations = groupToNumOfInvocationsOfGetOperations.get(group);
  //        groupToNumOfInvocationsOfGetOperations.put(group, numOfInvocations + 1);
  //      }
  //    }
  //    for (int i = 0; i < groupToNumOfInvocationsOfGetOperations.get(group1); i++) {
  //      verify(MockPipeOpManager, times(1))
  //          .getOperations(
  //              group1,
  //              (long) i * externalPipeConfiguration.getOperationBatchSize(),
  //              externalPipeConfiguration.getOperationBatchSize());
  //    }
  //    for (int i = 0; i < groupToNumOfInvocationsOfGetOperations.get(group2); i++) {
  //      verify(MockPipeOpManager, times(1))
  //          .getOperations(
  //              group1,
  //              (long) i * externalPipeConfiguration.getOperationBatchSize(),
  //              externalPipeConfiguration.getOperationBatchSize());
  //    }
  //
  //    // Verify that the insert methods of writer get called as expected.
  //    verify(
  //            mockExternalPipeSinkWriter,
  //            times(
  //
  // groupToNumOfInvocationsOfGetOperations.values().stream().reduce(Integer::sum).get()
  //                    * externalPipeConfiguration.getOperationBatchSize()))
  //        .insertFloat(any(), anyLong(), anyFloat());
  //  }
  //
  //  /** Test if the retry works as expected when writer throws {@link IOException}. */
  //  @Test(timeout = 1_000L)
  //  public void testExceptionHandlingForOpen() throws IOException {
  //    String errorMsg = "Failed to open";
  //    doThrow(new IOException(errorMsg)).when(mockExternalPipeSinkWriter).open();
  //    // Run the pipe.
  //    ExternalPipeConfiguration externalPipeConfiguration =
  //        new ExternalPipeConfiguration.Builder("TestPipe").numOfThreads(1).build();
  //    ExternalPipe externalPipe =
  //        new ExternalPipe(
  //            "Test",
  //            MockPipeOpManager,
  //            mockExternalPipeSinkWriterFactory,
  //            externalPipeConfiguration);
  //    try {
  //      externalPipe.start();
  //      fail("Expect an IOException");
  //    } catch (IOException e) {
  //      Assert.assertEquals(errorMsg, e.getMessage());
  //    }
  //  }
  //
  //  /** Test if the retry works as expected when writer throws {@link IOException}. */
  //  @Test(timeout = 5_000L)
  //  public void testExceptionHandlingForInsert() throws IOException, InterruptedException {
  //    String errorMsg = "Failed to insert";
  //    doThrow(new IOException(errorMsg))
  //        .when(mockExternalPipeSinkWriter)
  //        .insertFloat(any(), anyLong(), anyFloat());
  //    // Run the pipe for a second.
  //    ExternalPipeConfiguration externalPipeConfiguration =
  //        new ExternalPipeConfiguration.Builder("TestPipe").numOfThreads(1).build();
  //    ExternalPipe externalPipe =
  //        new ExternalPipe(
  //            "Test",
  //            MockPipeOpManager,
  //            mockExternalPipeSinkWriterFactory,
  //            externalPipeConfiguration);
  //    externalPipe.start();
  //    Thread.sleep(1_000L);
  //    externalPipe.stop();
  //
  //    Assert.assertEquals(
  //        1, externalPipe.getStatus().getWriterInvocationFailures().get("insert").size());
  //    Assert.assertTrue(
  //        externalPipe.getStatus().getWriterInvocationFailures().get("insert").get(errorMsg).get()
  //            > 0);
  //
  //    // Verify that the times of invocation for each operation equals the configured attempt
  // times.
  //    Set<List<String>> paths = new HashSet<>();
  //    for (Invocation invocation : mockingDetails(mockExternalPipeSinkWriter).getInvocations()) {
  //      if ("insertFloat".equals(invocation.getMethod().getName())) {
  //        try {
  //          verify(mockExternalPipeSinkWriter, times(externalPipeConfiguration.getAttemptTimes()))
  //              .insertFloat(
  //                  invocation.getArgument(0), invocation.getArgument(1),
  // invocation.getArgument(2));
  //        } catch (TooLittleActualInvocations e) {
  //          // Retry could be interrupted by the stop of external pipe.
  //          paths.add(Arrays.asList(invocation.getArgument(0)));
  //        }
  //      }
  //    }
  //    Assert.assertEquals(paths.toString(), 1, paths.size());
  //  }
  //
  //  /** Test if the retry works as expected when writer throws {@link IOException}. */
  //  @Test(timeout = 5_000L)
  //  public void testExceptionHandlingForDelete() {
  //    // TODO
  //  }
  //
  //  /** Test if the retry works as expected when writer throws {@link IOException}. */
  //  @Test(timeout = 5_000L)
  //  public void testExceptionHandlingForCreateTimeSeries() {
  //    // TODO
  //  }
  //
  //  /** Test if the retry works as expected when writer throws {@link IOException}. */
  //  @Test(timeout = 5_000L)
  //  public void testExceptionHandlingForDeleteTimeSeries() {
  //    // TODO
  //  }
  //
  //  /** Test if the retry works as expected when writer throws {@link IOException}. */
  //  @Test(timeout = 5_000L)
  //  public void testExceptionHandlingForFlush() throws IOException, InterruptedException {
  //    String errorMsg = "Failed to flush";
  //    doThrow(new IOException(errorMsg)).when(mockExternalPipeSinkWriter).flush();
  //    // Run the pipe for a second.
  //    ExternalPipeConfiguration externalPipeConfiguration =
  //        new ExternalPipeConfiguration.Builder("TestPipe").numOfThreads(1).build();
  //    ExternalPipe externalPipe =
  //        new ExternalPipe(
  //            "Test",
  //            MockPipeOpManager,
  //            mockExternalPipeSinkWriterFactory,
  //            externalPipeConfiguration);
  //    externalPipe.start();
  //    Thread.sleep(1_000L);
  //    externalPipe.stop();
  //
  //    Assert.assertEquals(
  //        1, externalPipe.getStatus().getWriterInvocationFailures().get("flush").size());
  //    Assert.assertTrue(
  //        externalPipe.getStatus().getWriterInvocationFailures().get("flush").get(errorMsg).get()
  //            > 0);
  //
  //    // The IOException thrown by flush should be ignored. Thus, the invocation of insert should
  // not
  //    // be affected.
  //    Iterator<Invocation> MockPipeOpManagerInvocationIterator =
  //        mockingDetails(MockPipeOpManager).getInvocations().iterator();
  //    int numOfInvocationsOfGetOperations = 0;
  //    while (MockPipeOpManagerInvocationIterator.hasNext()) {
  //      Invocation invocation = MockPipeOpManagerInvocationIterator.next();
  //      if (invocation.getMethod().getName().equals("getOperations")) {
  //        numOfInvocationsOfGetOperations += 1;
  //      }
  //    }
  //    verify(
  //            mockExternalPipeSinkWriter,
  //            times(
  //                numOfInvocationsOfGetOperations
  //                    * externalPipeConfiguration.getOperationBatchSize()))
  //        .insertFloat(any(), anyLong(), anyFloat());
  //  }
  //
  //  /** Test if the retry works as expected when writer throws {@link IOException}. */
  //  @Test(timeout = 5_000L)
  //  public void testExceptionHandlingForClose() throws IOException, InterruptedException {
  //    String errorMsg = "Failed to close";
  //    doThrow(new IOException(errorMsg)).when(mockExternalPipeSinkWriter).close();
  //
  //    // Run the pipe for a second.
  //    ExternalPipeConfiguration externalPipeConfiguration =
  //        new ExternalPipeConfiguration.Builder("TestPipe").numOfThreads(1).build();
  //    ExternalPipe externalPipe =
  //        new ExternalPipe(
  //            "Test",
  //            MockPipeOpManager,
  //            mockExternalPipeSinkWriterFactory,
  //            externalPipeConfiguration);
  //    externalPipe.start();
  //    Thread.sleep(1_000L);
  //    externalPipe.stop();
  //
  //    Assert.assertEquals(
  //        1, externalPipe.getStatus().getWriterInvocationFailures().get("close").size());
  //    Assert.assertTrue(
  //        externalPipe.getStatus().getWriterInvocationFailures().get("close").get(errorMsg).get()
  //            > 0);
  //
  //    // The IOException thrown by close should be ignored. Thus, the invocation of insert should
  // not
  //    // be affected.
  //    Iterator<Invocation> MockPipeOpManagerInvocationIterator =
  //        mockingDetails(MockPipeOpManager).getInvocations().iterator();
  //    int numOfInvocationsOfGetOperations = 0;
  //    while (MockPipeOpManagerInvocationIterator.hasNext()) {
  //      Invocation invocation = MockPipeOpManagerInvocationIterator.next();
  //      if (invocation.getMethod().getName().equals("getOperations")) {
  //        numOfInvocationsOfGetOperations += 1;
  //      }
  //    }
  //    verify(
  //            mockExternalPipeSinkWriter,
  //            times(
  //                numOfInvocationsOfGetOperations
  //                    * externalPipeConfiguration.getOperationBatchSize()))
  //        .insertFloat(any(), anyLong(), anyFloat());
  //  }
  //
  //  @Test(timeout = 5_000L)
  //  public void testSystemExceptionHandling() {
  //    // TODO
  //  }
}
