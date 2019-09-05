/**
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

package org.apache.iotdb.db.qp.plan.logicalPlan;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.qp.logical.ExecutableOperator;
import org.apache.iotdb.db.qp.strategy.LogicalGenerator;
import org.junit.Before;
import org.junit.Test;

public class TestIndexStatement {
  private LogicalGenerator generator;

  @Before
  public void before() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    generator = new LogicalGenerator(config.getZoneID());
  }

  @Test (expected = NullPointerException.class)
  // it contains where clause. The where clause will be added into the overall operator, which is null
  public void createIndex1() {
    ExecutableOperator op = generator.getLogicalPlan(
            "create index on root.a.b.c using kvindex with window_length=50 where time > 123");
  }

  @Test (expected = NullPointerException.class)
  public void createIndex2() {
    ExecutableOperator op = generator.getLogicalPlan(
            "create index on root.a.b.c using kv-match2 with xxx=50,xxx=123 where time > now();");
  }

  @Test
  public void dropIndex() {
    ExecutableOperator op = generator.getLogicalPlan("drop index kvindex on root.a.b.c");
  }
}
