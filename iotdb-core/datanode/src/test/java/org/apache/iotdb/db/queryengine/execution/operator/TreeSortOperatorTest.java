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

package org.apache.iotdb.db.queryengine.execution.operator;

import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.NonAlignedFullPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.process.TreeSortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.FullOuterTimeJoinOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.AscTimeComparator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.DescTimeComparator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.SingleColumnMerger;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.InputLocation;
import org.apache.iotdb.db.queryengine.plan.planner.plan.parameter.SeriesScanOptions;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.storageengine.dataregion.read.QueryDataSource;
import org.apache.iotdb.db.storageengine.dataregion.read.reader.series.SeriesReaderTestUtil;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.utils.datastructure.SortKey;

import io.airlift.units.Duration;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.utils.EnvironmentUtils.cleanDir;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TreeSortOperatorTest {

  private static final String SORT_OPERATOR_TEST_SG = "root.TreeSortOperatorTest";
  private final List<String> deviceIds = new ArrayList<>();
  private final List<IMeasurementSchema> measurementSchemas = new ArrayList<>();

  private final List<TsFileResource> seqResources = new ArrayList<>();
  private final List<TsFileResource> unSeqResources = new ArrayList<>();

  private final String sortTmpPrefixPath =
      "target" + File.separator + "sort" + File.separator + "tmp";

  private int dataNodeId;

  private int maxTsBlockSizeInBytes;

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    dataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    maxTsBlockSizeInBytes = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(200);
    SeriesReaderTestUtil.setUp(
        measurementSchemas, deviceIds, seqResources, unSeqResources, SORT_OPERATOR_TEST_SG);
  }

  @After
  public void tearDown() throws IOException {
    SeriesReaderTestUtil.tearDown(seqResources, unSeqResources);
    cleanDir(sortTmpPrefixPath);
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(dataNodeId);
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(maxTsBlockSizeInBytes);
  }

  // ------------------------------------------------------------------------------------------------
  //                                   sortOperatorTest
  // ------------------------------------------------------------------------------------------------
  //                                      TreeSortOperator
  //                                           |
  //                                    TimeJoinOperator
  //                      _____________________|______________________________
  //                     /                     |                              \
  //        SeriesScanOperator      TimeJoinOperator                TimeJoinOperator
  //                                  /                \              /               \
  //                  SeriesScanOperator SeriesScanOperator SeriesScanOperator   SeriesScanOperator
  // ------------------------------------------------------------------------------------------------
  public Operator genSortOperator(Ordering timeOrdering, boolean getSortOperator) {
    ExecutorService instanceNotificationExecutor =
        IoTDBThreadPoolFactory.newFixedThreadPool(1, "treeSortOperator-test-instance-notification");
    // Construct operator tree
    QueryId queryId = new QueryId("stub_query");

    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(instanceId, instanceNotificationExecutor);
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId planNodeId1 = new PlanNodeId("1");
    driverContext.addOperatorContext(1, planNodeId1, SeriesScanOperator.class.getSimpleName());
    PlanNodeId planNodeId2 = new PlanNodeId("2");
    driverContext.addOperatorContext(2, planNodeId2, SeriesScanOperator.class.getSimpleName());
    driverContext.addOperatorContext(
        3, new PlanNodeId("3"), FullOuterTimeJoinOperator.class.getSimpleName());
    driverContext.addOperatorContext(
        4, new PlanNodeId("4"), TreeSortOperator.class.getSimpleName());

    NonAlignedFullPath measurementPath1 =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(SORT_OPERATOR_TEST_SG + ".device0"),
            new MeasurementSchema("sensor0", TSDataType.INT32));
    NonAlignedFullPath measurementPath2 =
        new NonAlignedFullPath(
            IDeviceID.Factory.DEFAULT_FACTORY.create(SORT_OPERATOR_TEST_SG + ".device1"),
            new MeasurementSchema("sensor0", TSDataType.INT32));

    SeriesScanOperator seriesScanOperator1 =
        new SeriesScanOperator(
            driverContext.getOperatorContexts().get(0),
            planNodeId1,
            measurementPath1,
            timeOrdering,
            SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath1));
    seriesScanOperator1.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
    seriesScanOperator1
        .getOperatorContext()
        .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

    SeriesScanOperator seriesScanOperator2 =
        new SeriesScanOperator(
            driverContext.getOperatorContexts().get(1),
            planNodeId2,
            measurementPath2,
            timeOrdering,
            SeriesScanOptions.getDefaultSeriesScanOptions(measurementPath2));
    seriesScanOperator2.initQueryDataSource(new QueryDataSource(seqResources, unSeqResources));
    seriesScanOperator2
        .getOperatorContext()
        .setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

    List<TSDataType> tsDataTypes =
        new LinkedList<>(Arrays.asList(TSDataType.INT32, TSDataType.INT32));

    FullOuterTimeJoinOperator timeJoinOperator1 =
        new FullOuterTimeJoinOperator(
            driverContext.getOperatorContexts().get(2),
            Arrays.asList(seriesScanOperator1, seriesScanOperator2),
            timeOrdering,
            Arrays.asList(TSDataType.INT32, TSDataType.INT32),
            Arrays.asList(
                new SingleColumnMerger(
                    new InputLocation(0, 0),
                    timeOrdering == Ordering.ASC
                        ? new AscTimeComparator()
                        : new DescTimeComparator()),
                new SingleColumnMerger(
                    new InputLocation(1, 0),
                    timeOrdering == Ordering.ASC
                        ? new AscTimeComparator()
                        : new DescTimeComparator())),
            timeOrdering == Ordering.ASC ? new AscTimeComparator() : new DescTimeComparator());
    timeJoinOperator1.getOperatorContext().setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));

    if (!getSortOperator) return timeJoinOperator1;

    Comparator<SortKey> comparator =
        Comparator.comparing(
            (SortKey sortKey) -> sortKey.tsBlock.getColumn(0).getInt(sortKey.rowIndex));

    OperatorContext operatorContext = driverContext.getOperatorContexts().get(3);
    String filePrefix =
        sortTmpPrefixPath
            + File.separator
            + operatorContext
                .getDriverContext()
                .getFragmentInstanceContext()
                .getId()
                .getFragmentInstanceId()
            + File.separator
            + operatorContext.getDriverContext().getPipelineId()
            + File.separator;
    TreeSortOperator treeSortOperator =
        new TreeSortOperator(
            operatorContext, timeJoinOperator1, tsDataTypes, filePrefix, comparator);
    treeSortOperator.getOperatorContext().setMaxRunTime(new Duration(500, TimeUnit.MILLISECONDS));
    return treeSortOperator;
  }

  long getValue(long expectedTime) {
    if (expectedTime < 200) {
      return 20000 + expectedTime;
    } else if (expectedTime < 260
        || (expectedTime >= 300 && expectedTime < 380)
        || expectedTime >= 400) {
      return 10000 + expectedTime;
    } else {
      return expectedTime;
    }
  }

  // with data spilling
  @Test
  public void sortOperatorSpillingTest() throws Exception {
    IoTDBDescriptor.getInstance().getConfig().setSortBufferSize(5000);
    long sortBufferSize = IoTDBDescriptor.getInstance().getConfig().getSortBufferSize();
    try (TreeSortOperator root = (TreeSortOperator) genSortOperator(Ordering.ASC, true)) {
      int lastValue = -1;
      int count = 0;
      while (root.isBlocked().isDone() && root.hasNext()) {
        TsBlock tsBlock = root.next();
        if (tsBlock == null) continue;
        for (int i = 0; i < tsBlock.getPositionCount(); i++) {
          long time = tsBlock.getTimeByIndex(i);
          int v1 = tsBlock.getColumn(0).getInt(i);
          int v2 = tsBlock.getColumn(1).getInt(i);
          assertTrue(lastValue == -1 || lastValue < v1);
          assertEquals(getValue(time), v1);
          assertEquals(v1, v2);
          lastValue = v1;
          count++;
        }
      }
      assertEquals(500, count);
    } finally {
      IoTDBDescriptor.getInstance().getConfig().setSortBufferSize(sortBufferSize);
    }
  }

  // no data spilling
  @Test
  public void sortOperatorNormalTest() throws Exception {
    try (Operator root = genSortOperator(Ordering.ASC, true)) {
      int lastValue = -1;
      int count = 0;
      while (root.isBlocked().isDone() && root.hasNext()) {
        TsBlock tsBlock = root.next();
        if (tsBlock == null) continue;
        for (int i = 0; i < tsBlock.getPositionCount(); i++) {
          long time = tsBlock.getTimeByIndex(i);
          int v1 = tsBlock.getColumn(0).getInt(i);
          int v2 = tsBlock.getColumn(1).getInt(i);
          assertTrue(lastValue == -1 || lastValue < v1);
          assertEquals(getValue(time), v1);
          assertEquals(v1, v2);
          lastValue = v1;
          count++;
        }
      }
      assertEquals(500, count);
    }
  }
}
