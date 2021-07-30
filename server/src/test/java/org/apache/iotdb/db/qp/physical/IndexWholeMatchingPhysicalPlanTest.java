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
package org.apache.iotdb.db.qp.physical;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.MManager;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.Planner;
import org.apache.iotdb.db.qp.physical.sys.CreateIndexPlan;
import org.apache.iotdb.db.qp.physical.sys.DropIndexPlan;
import org.apache.iotdb.db.utils.EnvironmentUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;

/** refer to org.apache.iotdb.db.qp.plan.PhysicalPlanTest */
public class IndexWholeMatchingPhysicalPlanTest {

  private Planner processor = new Planner();

  @Before
  public void before() throws MetadataException {
    MManager.getInstance().init();
    MManager.getInstance().setStorageGroup(new PartialPath("root.Ery"));
    MManager.getInstance()
        .createTimeseries(
            new PartialPath("root.Ery.Ferm01.Glu"),
            TSDataType.FLOAT,
            TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED,
            null);
    MManager.getInstance()
        .createTimeseries(
            new PartialPath("root.Ery.Ferm02.Glu"),
            TSDataType.FLOAT,
            TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED,
            null);
    MManager.getInstance()
        .createTimeseries(
            new PartialPath("root.Ery.Ferm03.Glu"),
            TSDataType.FLOAT,
            TSEncoding.PLAIN,
            CompressionType.UNCOMPRESSED,
            null);
  }

  @After
  public void clean() throws IOException {
    MManager.getInstance().clear();
    EnvironmentUtils.cleanAllDir();
  }

  @Test
  public void testCreateIndex() throws QueryProcessException {
    String sqlStr =
        "CREATE INDEX ON root.Ery.*.Glu WHERE time > 50 WITH INDEX=RTREE_PAA, PAA_DIM=8";

    Planner processor = new Planner();
    CreateIndexPlan plan = (CreateIndexPlan) processor.parseSQLToPhysicalPlan(sqlStr);
    assertEquals(
        "paths: [root.Ery.*.Glu], index type: RTREE_PAA, start time: 50, props: {PAA_DIM=8}",
        plan.toString());
  }

  @Test
  public void testDropIndex() throws QueryProcessException {
    String sqlStr = "DROP INDEX RTREE_PAA ON root.Ery.*.Glu";
    Planner processor = new Planner();
    DropIndexPlan plan = (DropIndexPlan) processor.parseSQLToPhysicalPlan(sqlStr);
    assertEquals("paths: [root.Ery.*.Glu], index type: RTREE_PAA", plan.toString());
  }

  @Test
  public void testCreateIndexSerialize()
      throws QueryProcessException, IOException, IllegalPathException {
    String sqlStr =
        "CREATE INDEX ON root.Ery.*.Glu WHERE time > 50 WITH INDEX=RTREE_PAA, PAA_DIM=8";
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

  @Test
  public void testDropIndexSerialize()
      throws QueryProcessException, IOException, IllegalPathException {
    String sqlStr = "DROP INDEX RTREE_PAA ON root.Ery.*.Glu";
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
