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

package org.apache.iotdb.db.qp.plan;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class SerializationTest {

  private Planner processor = new Planner();

  @Before
  public void before() throws MetadataException {
    MManager.getInstance().init();
    MManager.getInstance().setStorageGroup(new PartialPath("root.vehicle"));
    MManager.getInstance()
        .createTimeseries(new PartialPath("root.vehicle.d1.s1"), TSDataType.FLOAT, TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED, null);
    MManager.getInstance()
        .createTimeseries(new PartialPath("root.vehicle.d2.s1"), TSDataType.FLOAT, TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED, null);
    MManager.getInstance()
        .createTimeseries(new PartialPath("root.vehicle.d3.s1"), TSDataType.FLOAT, TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED, null);
    MManager.getInstance()
        .createTimeseries(new PartialPath("root.vehicle.d4.s1"), TSDataType.FLOAT, TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED, null);
  }

  @After
  public void clean() throws IOException {
    MManager.getInstance().clear();
    EnvironmentUtils.cleanAllDir();
  }

  @Test
  public void testInsert() throws QueryProcessException, IOException, IllegalPathException {
    String sqlStr = "INSERT INTO root.vehicle.d1(timestamp, s1) VALUES (1, 5.0)";
    PhysicalPlan plan = processor.parseSQLToPhysicalPlan(sqlStr);
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    try (DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream)) {
      plan.serialize(dataOutputStream);
      ByteBuffer buffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
      PhysicalPlan planB = PhysicalPlan.Factory.create(buffer);
      assertEquals(plan, planB);
    }

    ByteBuffer buffer = ByteBuffer.allocate(4096);
    plan.serialize(buffer);
    buffer.flip();
    PhysicalPlan planB = PhysicalPlan.Factory.create(buffer);
    assertEquals(plan, planB);
  }
}
