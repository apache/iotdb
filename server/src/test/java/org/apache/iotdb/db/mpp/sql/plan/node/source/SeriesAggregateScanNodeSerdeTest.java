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

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.partition.RegionReplicaSet;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.mpp.sql.plan.node.PlanNodeDeserializeHelper;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.sql.planner.plan.node.source.SeriesAggregateScanNode;
import org.apache.iotdb.db.mpp.sql.statement.component.OrderBy;
import org.apache.iotdb.db.query.aggregation.AggregationType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.operator.In;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.tsfile.read.filter.factory.FilterType.VALUE_FILTER;
import static org.junit.Assert.assertEquals;

public class SeriesAggregateScanNodeSerdeTest {
  @Test
  public void TestSerializeAndDeserialize() throws QueryProcessException, IllegalPathException {
    Set<String> st = new HashSet<>();
    st.add("s1");
    st.add("s2");
    List<AggregationType> aggregateFuncList = new ArrayList<>();
    aggregateFuncList.add(AggregationType.MAX_TIME);
    SeriesAggregateScanNode seriesAggregateScanNode =
        new SeriesAggregateScanNode(
            new PlanNodeId("TestSeriesAggregateScanNode"),
            new MeasurementPath("root.sg.d1.s1", TSDataType.BOOLEAN),
            aggregateFuncList,
            OrderBy.TIMESTAMP_ASC,
            new In<String>(st, VALUE_FILTER, true),
            null);
    seriesAggregateScanNode.setRegionReplicaSet(
        new RegionReplicaSet(new DataRegionId(1), new ArrayList<>()));

    ByteBuffer byteBuffer = ByteBuffer.allocate(2048);
    seriesAggregateScanNode.serialize(byteBuffer);
    byteBuffer.flip();
    assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), seriesAggregateScanNode);
  }
}
