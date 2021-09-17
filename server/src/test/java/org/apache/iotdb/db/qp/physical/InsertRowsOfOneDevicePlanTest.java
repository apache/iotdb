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
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.qp.physical.crud.InsertRowPlan;
import org.apache.iotdb.db.qp.physical.crud.InsertRowsOfOneDevicePlan;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class InsertRowsOfOneDevicePlanTest {

  @Test
  public void testSerializable() throws IllegalPathException, IOException {

    PartialPath device = new PartialPath("root.sg.d");
    InsertRowPlan[] rowPlans =
        new InsertRowPlan[] {
          new InsertRowPlan(
              device,
              1000L,
              new String[] {"s1", "s2", "s3"},
              new TSDataType[] {TSDataType.DOUBLE, TSDataType.FLOAT, TSDataType.INT64},
              new String[] {"1.0", "2", "300"},
              true),
          new InsertRowPlan(
              device,
              2000L,
              new String[] {"s1", "s4"},
              new TSDataType[] {TSDataType.DOUBLE, TSDataType.TEXT},
              new String[] {"2.0", "abc"},
              true),
        };

    InsertRowsOfOneDevicePlan p = new InsertRowsOfOneDevicePlan(device, rowPlans, new int[] {0, 1});

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    DataOutputStream w = new DataOutputStream(baos);
    p.serialize(w);
    w.flush();
    byte[] res = baos.toByteArray();
    ByteBuffer buf = ByteBuffer.wrap(res);
    InsertRowsOfOneDevicePlan p2 = (InsertRowsOfOneDevicePlan) PhysicalPlan.Factory.create(buf);
    Assert.assertEquals(p, p2);
    res = new byte[1024];
    p.serialize(ByteBuffer.wrap(res));
    buf = ByteBuffer.wrap(res);
    p2 = (InsertRowsOfOneDevicePlan) PhysicalPlan.Factory.create(buf);
    Assert.assertEquals(p, p2);
  }
}
