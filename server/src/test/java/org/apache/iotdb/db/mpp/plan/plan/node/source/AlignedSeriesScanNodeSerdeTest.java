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

package org.apache.iotdb.db.mpp.plan.plan.node.source;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.AlignedPath;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.mpp.plan.planner.plan.node.source.AlignedSeriesScanNode;
import org.apache.iotdb.db.mpp.plan.statement.component.Ordering;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.filter.GroupByFilter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;

public class AlignedSeriesScanNodeSerdeTest {
  @Test
  public void testSerializeAndDeserialize() throws QueryProcessException, IllegalPathException {
    AlignedSeriesScanNode seriesScanNode =
        new AlignedSeriesScanNode(
            new PlanNodeId("3000000"),
            new AlignedPath(
                "root.iot.DC004HP1MCY01M0008221075DB_999140",
                Arrays.asList("q", "v"),
                Arrays.asList(
                    new MeasurementSchema("q", TSDataType.INT32),
                    new MeasurementSchema("v", TSDataType.INT32))),
            Ordering.DESC,
            new GroupByFilter(1, 2, 3, 4),
            null,
            100,
            100,
            null);

    ByteBuffer byteBuffer = ByteBuffer.allocate(2048);
    System.out.println(byteBuffer.position());
    seriesScanNode.serialize(byteBuffer);
    System.out.println(byteBuffer.position());
    byteBuffer.flip();
    // assertEquals(PlanNodeDeserializeHelper.deserialize(byteBuffer), seriesScanNode);
  }
}
