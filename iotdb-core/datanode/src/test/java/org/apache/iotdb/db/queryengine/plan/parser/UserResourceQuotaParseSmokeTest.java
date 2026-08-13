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
package org.apache.iotdb.db.queryengine.plan.parser;

import org.apache.iotdb.common.rpc.thrift.TResourceType;
import org.apache.iotdb.common.rpc.thrift.TUserResourceQuota;
import org.apache.iotdb.common.rpc.thrift.ThrottleType;
import org.apache.iotdb.commons.exception.SemanticException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.DeleteUserResourceQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.SetUserResourceQuotaStatement;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.ShowUserResourceQuotaStatement;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.time.ZoneId;

public class UserResourceQuotaParseSmokeTest {

  private boolean oldEnable;

  @Before
  public void setUp() {
    oldEnable = IoTDBDescriptor.getInstance().getConfig().isQuotaEnable();
    IoTDBDescriptor.getInstance().getConfig().setQuotaEnable(true);
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setQuotaEnable(oldEnable);
  }

  @Test
  public void validUnitsParse() {
    // disk_io: bytes/sec; memory/temp_disk: bytes (no size suffix in USER QUOTA).
    Statement s =
        StatementGenerator.createStatement(
            "SET USER QUOTA ON u1 WITH read_cpu_min=1, read_cpu_max=4, write_memory_max=8589934592, write_disk_io_max=10485760",
            ZoneId.systemDefault());
    Assert.assertTrue(s instanceof SetUserResourceQuotaStatement);
    TUserResourceQuota q = ((SetUserResourceQuotaStatement) s).getUserResourceQuota();
    Assert.assertEquals(1, q.getReadQuota().get(TResourceType.CPU).getMinValue());
    Assert.assertEquals(4, q.getReadQuota().get(TResourceType.CPU).getMaxValue());
    Assert.assertEquals(
        8L * 1024 * 1024 * 1024, q.getWriteQuota().get(TResourceType.MEMORY).getMaxValue());
    Assert.assertTrue(q.getThrottleLimit().containsKey(ThrottleType.WRITE_SIZE));
    Assert.assertEquals(
        10L * 1024 * 1024, q.getThrottleLimit().get(ThrottleType.WRITE_SIZE).getSoftLimit());
  }

  @Test
  public void tempDiskAttrParses() {
    // temp_disk fixed unit: bytes long.
    Statement s =
        StatementGenerator.createStatement(
            "SET USER QUOTA ON u1 WITH read_temp_disk_max=10737418240, write_temp_disk_min=1073741824",
            ZoneId.systemDefault());
    Assert.assertTrue(s instanceof SetUserResourceQuotaStatement);
    TUserResourceQuota q = ((SetUserResourceQuotaStatement) s).getUserResourceQuota();
    Assert.assertEquals(
        10L * 1024 * 1024 * 1024, q.getReadQuota().get(TResourceType.TEMP_DISK).getMaxValue());
    Assert.assertEquals(
        1L * 1024 * 1024 * 1024, q.getWriteQuota().get(TResourceType.TEMP_DISK).getMinValue());
  }

  @Test
  public void memorySizeSuffixRejected() {
    try {
      StatementGenerator.createStatement(
          "SET USER QUOTA ON u1 WITH write_memory_max=8G", ZoneId.systemDefault());
      Assert.fail("expected parse failure for size suffix on memory");
    } catch (SemanticException | NumberFormatException e) {
      // expected: memory is positive long bytes only
    }
  }

  @Test
  public void showUserQuotaParses() {
    Statement s = StatementGenerator.createStatement("SHOW USER QUOTA u1", ZoneId.systemDefault());
    Assert.assertTrue(s instanceof ShowUserResourceQuotaStatement);
    ShowUserResourceQuotaStatement show = (ShowUserResourceQuotaStatement) s;
    Assert.assertEquals("u1", show.getUserName());
  }

  @Test
  public void deleteUserQuotaParses() {
    Statement s =
        StatementGenerator.createStatement("DELETE USER QUOTA ON u1", ZoneId.systemDefault());
    Assert.assertTrue(s instanceof DeleteUserResourceQuotaStatement);
    Assert.assertEquals("u1", ((DeleteUserResourceQuotaStatement) s).getUserName());
  }

  @Test
  public void quotaDisabledRejected() {
    IoTDBDescriptor.getInstance().getConfig().setQuotaEnable(false);
    try {
      StatementGenerator.createStatement(
          "SET USER QUOTA ON u1 WITH read_cpu_max=1", ZoneId.systemDefault());
      Assert.fail("expected quota_enable=false to reject SET");
    } catch (SemanticException e) {
      Assert.assertTrue(e.getMessage().toLowerCase().contains("enable"));
    }
  }

  @Test
  public void rootUserRejected() {
    try {
      StatementGenerator.createStatement(
          "SET USER QUOTA ON `root` WITH read_cpu_max=1", ZoneId.systemDefault());
      Assert.fail("expected root SET to fail");
    } catch (SemanticException e) {
      // expected
    }
    try {
      StatementGenerator.createStatement("DELETE USER QUOTA ON `root`", ZoneId.systemDefault());
      Assert.fail("expected root DELETE to fail");
    } catch (SemanticException e) {
      // expected
    }
  }

  @Test
  public void invalidAttrAndDiskIoRejected() {
    try {
      StatementGenerator.createStatement(
          "SET USER QUOTA ON u1 WITH read_cpu_xx=1", ZoneId.systemDefault());
      Assert.fail("expected invalid attr");
    } catch (RuntimeException e) {
      // SemanticException / IllegalArgumentException
    }
    try {
      StatementGenerator.createStatement(
          "SET USER QUOTA ON u1 WITH write_disk_io_max='10M/sec'", ZoneId.systemDefault());
      Assert.fail("expected invalid disk_io complex unit");
    } catch (RuntimeException e) {
      // SemanticException: disk_io must be positive long bytes/sec
    }
    try {
      StatementGenerator.createStatement(
          "SET USER QUOTA ON u1 WITH write_disk_io_min=10485760", ZoneId.systemDefault());
      Assert.fail("expected disk_io_min to be rejected");
    } catch (SemanticException e) {
      // expected: only *_disk_io_max is valid
    }
    try {
      StatementGenerator.createStatement(
          "SET USER QUOTA ON u1 WITH write_temp_disk_max=8G", ZoneId.systemDefault());
      Assert.fail("expected invalid temp_disk size suffix");
    } catch (RuntimeException e) {
      // SemanticException: temp_disk must be positive long bytes
    }
    try {
      StatementGenerator.createStatement(
          "SET USER QUOTA ON u1 WITH read_cpu_max=0", ZoneId.systemDefault());
      Assert.fail("expected non-positive cpu quota to be rejected");
    } catch (SemanticException e) {
      // expected
    }
  }
}
