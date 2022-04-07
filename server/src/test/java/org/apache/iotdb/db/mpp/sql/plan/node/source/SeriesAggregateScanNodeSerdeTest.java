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
package org.apache.iotdb.db.mpp.sql.plan.node.source;


import static org.apache.iotdb.tsfile.read.filter.factory.FilterType.VALUE_FILTER;
import static org.junit.Assert.assertEquals;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Set;
import org.apache.iotdb.commons.cluster.Endpoint;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.consensus.GroupType;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.common.FragmentInstanceId;
import org.apache.iotdb.db.mpp.common.GroupByTimeParameter;
import org.apache.iotdb.db.mpp.common.PlanFragmentId;
import org.apache.iotdb.db.mpp.sql.constant.FilterConstant.FilterType;
import org.apache.iotdb.db.mpp.sql.plan.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.metedata.read.ShowDevicesNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.sink.FragmentSinkNode;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesAggregateScanNode;
import org.apache.iotdb.db.qp.utils.GroupByLevelController;
import org.apache.iotdb.db.query.expression.unary.FunctionExpression;
import org.apache.iotdb.tsfile.read.expression.impl.GlobalTimeExpression;
import org.apache.iotdb.tsfile.read.expression.impl.SingleSeriesExpression;
import org.apache.iotdb.tsfile.read.filter.operator.In;
import org.apache.iotdb.tsfile.read.filter.operator.Like;
import org.junit.Test;

public class SeriesAggregateScanNodeSerdeTest {
  @Test
  public void TestSerializeAndDeserialize() throws QueryProcessException, IllegalPathException {
    GroupByTimeParameter groupByTimeParameter = new GroupByTimeParameter();
    groupByTimeParameter.setEndTime(111111);
    Set<String> st = new HashSet<String>();
    st.add("s1"); st.add("s2");
    groupByTimeParameter.setExpression(new SingleSeriesExpression(new MeasurementPath("s"), new In<String>(st, VALUE_FILTER, true))); // new Like<String>("s1", VALUE_FILTER)
    groupByTimeParameter.setGroupByLevelController(new GroupByLevelController(1, new int[] {1, 3}));
    SeriesAggregateScanNode seriesAggregateScanNode = new SeriesAggregateScanNode(new PlanNodeId("TestSeriesAggregateScanNode"), new FunctionExpression("add"), groupByTimeParameter);
    seriesAggregateScanNode.setFilter(new In<String>(st, VALUE_FILTER, true));
    seriesAggregateScanNode.setDataRegionReplicaSet(new RegionReplicaSet(new ConsensusGroupId(GroupType.DataRegion, 1), new ArrayList<>()));

    ByteBuffer byteBuffer = ByteBuffer.allocate(2048);
    seriesAggregateScanNode.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), seriesAggregateScanNode);
  }
}
