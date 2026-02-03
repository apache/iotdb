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
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.FragmentInstanceId;
import org.apache.iotdb.db.queryengine.common.PlanFragmentId;
import org.apache.iotdb.db.queryengine.common.QueryId;
import org.apache.iotdb.db.queryengine.execution.driver.DriverContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceStateMachine;
import org.apache.iotdb.db.queryengine.execution.operator.process.SortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.TreeSortOperator;
import org.apache.iotdb.db.queryengine.execution.operator.process.join.merge.MergeSortComparator;
import org.apache.iotdb.db.queryengine.execution.operator.source.SeriesScanOperator;
import org.apache.iotdb.db.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.queryengine.plan.statement.component.OrderByKey;
import org.apache.iotdb.db.queryengine.plan.statement.component.Ordering;
import org.apache.iotdb.db.queryengine.plan.statement.component.SortItem;
import org.apache.iotdb.db.utils.datastructure.SortKey;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.block.column.ColumnBuilder;
import org.apache.tsfile.common.conf.TSFileDescriptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.read.common.block.TsBlock;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

import static org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext.createFragmentInstanceContext;
import static org.apache.iotdb.db.utils.EnvironmentUtils.cleanDir;
import static org.junit.Assert.assertEquals;

public class SortOperatorSortBranchTest {
  private final String sortDir = "target" + File.separator + "sort";
  private final String sortTmpPrefixPath = sortDir + File.separator + "tmp";

  private int dataNodeId;

  private int maxTsBlockSizeInBytes;

  private long sortBufferSize;

  @Before
  public void setUp() throws MetadataException, IOException, WriteProcessException {
    dataNodeId = IoTDBDescriptor.getInstance().getConfig().getDataNodeId();
    maxTsBlockSizeInBytes = TSFileDescriptor.getInstance().getConfig().getMaxTsBlockSizeInBytes();
    sortBufferSize = IoTDBDescriptor.getInstance().getConfig().getSortBufferSize();
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(0);
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(15);
    IoTDBDescriptor.getInstance().getConfig().setSortBufferSize(150);
  }

  @After
  public void tearDown() throws IOException {
    cleanDir(sortDir);
    IoTDBDescriptor.getInstance().getConfig().setDataNodeId(dataNodeId);
    TSFileDescriptor.getInstance().getConfig().setMaxTsBlockSizeInBytes(maxTsBlockSizeInBytes);
    IoTDBDescriptor.getInstance().getConfig().setSortBufferSize(sortBufferSize);
  }

  private SortOperator genSortOperator() {

    // Construct operator tree
    QueryId queryId = new QueryId("stub_query");

    FragmentInstanceId instanceId =
        new FragmentInstanceId(new PlanFragmentId(queryId, 0), "stub-instance");
    FragmentInstanceStateMachine stateMachine =
        new FragmentInstanceStateMachine(
            instanceId,
            IoTDBThreadPoolFactory.newFixedThreadPool(
                1, "sort-operator-test-instance-notification"));
    FragmentInstanceContext fragmentInstanceContext =
        createFragmentInstanceContext(instanceId, stateMachine);
    DriverContext driverContext = new DriverContext(fragmentInstanceContext, 0);
    PlanNodeId planNodeId1 = new PlanNodeId("1");
    driverContext.addOperatorContext(1, planNodeId1, SeriesScanOperator.class.getSimpleName());
    PlanNodeId planNodeId2 = new PlanNodeId("2");
    driverContext.addOperatorContext(2, planNodeId2, TreeSortOperator.class.getSimpleName());
    List<TSDataType> outputTypes = ImmutableList.of(TSDataType.INT32);

    Operator childOperator =
        new Operator() {
          int index = 0;
          private final List<int[]> data =
              ImmutableList.of(
                  new int[] {
                    20, 20, 21, 22, 23, 24, 25, 26,
                  },
                  new int[] {});

          @Override
          public OperatorContext getOperatorContext() {
            return driverContext.getOperatorContexts().get(0);
          }

          @Override
          public TsBlock next() {
            TsBlockBuilder builder = new TsBlockBuilder(outputTypes);
            int[] currentData = data.get(index);
            index++;

            ColumnBuilder[] columnBuilders = builder.getValueColumnBuilders();
            for (int i = 0; i < currentData.length; i++) {
              columnBuilders[0].writeInt(currentData[i]);
              builder.getTimeColumnBuilder().writeInt(currentData[i]);
            }
            builder.declarePositions(currentData.length);
            TsBlock result = builder.build();
            return result;
          }

          @Override
          public boolean hasNext() throws Exception {
            return !isFinished();
          }

          @Override
          public void close() throws Exception {}

          @Override
          public boolean isFinished() throws Exception {
            return index == data.size();
          }

          @Override
          public long calculateMaxPeekMemory() {
            return 0;
          }

          @Override
          public long calculateMaxReturnSize() {
            return 0;
          }

          @Override
          public long calculateRetainedSizeAfterCallingNext() {
            return 0;
          }

          @Override
          public long ramBytesUsed() {
            return 0;
          }
        };

    OperatorContext operatorContext = driverContext.getOperatorContexts().get(1);
    Comparator<SortKey> comparator =
        MergeSortComparator.getComparator(
            Arrays.asList(new SortItem(OrderByKey.DATANODEID, Ordering.ASC)),
            ImmutableList.of(0),
            outputTypes);
    return new TreeSortOperator(
        operatorContext,
        childOperator,
        ImmutableList.of(TSDataType.INT32),
        sortTmpPrefixPath,
        comparator);
  }

  @Test
  public void sortTest() throws Exception {
    SortOperator operator = genSortOperator();
    int[] expected =
        new int[] {
          20, 20, 21, 22, 23, 24, 25, 26,
        };
    int index = 0;
    while (operator.hasNext()) {
      TsBlock block = operator.next();
      if (block != null) {
        for (int i = 0; i < block.getPositionCount(); i++, index++) {
          assertEquals(expected[index], block.getColumn(0).getInt(i));
        }
      }
    }
  }
}
