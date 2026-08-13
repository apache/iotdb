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
package org.apache.iotdb.db.queryengine.plan.relational.sql.parser;

import org.apache.iotdb.common.rpc.thrift.TResourceType;
import org.apache.iotdb.common.rpc.thrift.TUserResourceQuota;
import org.apache.iotdb.common.rpc.thrift.ThrottleType;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.commons.queryengine.plan.relational.sql.ast.Statement;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.protocol.session.IClientSession;
import org.apache.iotdb.db.protocol.session.InternalClientSession;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.DeleteUserResourceQuota;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.SetUserResourceQuota;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.ShowUserResourceQuota;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.DeleteUserResourceQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.SetUserResourceQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.ShowUserResourceQuotaStatement;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;

public class UserResourceQuotaTableParseSmokeTest {

  private boolean oldEnable;
  private SqlParser sqlParser;
  private IClientSession clientSession;

  @Before
  public void setUp() {
    oldEnable = IoTDBDescriptor.getInstance().getConfig().isQuotaEnable();
    IoTDBDescriptor.getInstance().getConfig().setQuotaEnable(true);
    sqlParser = new SqlParser();
    clientSession = new InternalClientSession("testClient");
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setQuotaEnable(oldEnable);
  }

  @Test
  public void validUnitsParse() {
    Statement s =
        sqlParser.createStatement(
            "SET USER QUOTA ON u1 WITH read_cpu_min=1, read_cpu_max=4, write_memory_max=10485760, write_disk_io_max=10485760",
            ZoneId.systemDefault(),
            clientSession);
    Assert.assertTrue(s instanceof SetUserResourceQuota);
    TUserResourceQuota q =
        ((SetUserResourceQuotaStatement) ((SetUserResourceQuota) s).getInnerTreeStatement())
            .getUserResourceQuota();
    Assert.assertEquals(1, q.getReadQuota().get(TResourceType.CPU).getMinValue());
    Assert.assertEquals(4, q.getReadQuota().get(TResourceType.CPU).getMaxValue());
    Assert.assertEquals(
        10L * 1024 * 1024, q.getWriteQuota().get(TResourceType.MEMORY).getMaxValue());
    Assert.assertTrue(q.getThrottleLimit().containsKey(ThrottleType.WRITE_SIZE));
  }

  @Test
  public void showAndDeleteParse() {
    Statement show =
        sqlParser.createStatement("SHOW USER QUOTA u1", ZoneId.systemDefault(), clientSession);
    Assert.assertTrue(show instanceof ShowUserResourceQuota);
    Assert.assertEquals(
        "u1",
        ((ShowUserResourceQuotaStatement) ((ShowUserResourceQuota) show).getInnerTreeStatement())
            .getUserName());

    Statement delete =
        sqlParser.createStatement("DELETE USER QUOTA ON u1", ZoneId.systemDefault(), clientSession);
    Assert.assertTrue(delete instanceof DeleteUserResourceQuota);
    Assert.assertEquals(
        "u1",
        ((DeleteUserResourceQuotaStatement)
                ((DeleteUserResourceQuota) delete).getInnerTreeStatement())
            .getUserName());
  }

  @Test
  public void rootAndDisabledRejected() {
    try {
      sqlParser.createStatement(
          "SET USER QUOTA ON root WITH read_cpu_max=1", ZoneId.systemDefault(), clientSession);
      Assert.fail("expected root SET to fail");
    } catch (SemanticException e) {
      // expected
    }

    IoTDBDescriptor.getInstance().getConfig().setQuotaEnable(false);
    try {
      sqlParser.createStatement(
          "SET USER QUOTA ON u1 WITH read_cpu_max=1", ZoneId.systemDefault(), clientSession);
      Assert.fail("expected quota_enable=false to reject SET");
    } catch (SemanticException e) {
      Assert.assertTrue(e.getMessage().toLowerCase().contains("enable"));
    }
  }
}
