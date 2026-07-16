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

package org.apache.iotdb.db.storageengine.rescon.quotas;

import org.apache.iotdb.common.rpc.thrift.TUserResourceQuota;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.quota.OperationType;
import org.apache.iotdb.commons.quota.ResourceQuotaRange;
import org.apache.iotdb.commons.quota.ResourceType;
import org.apache.iotdb.commons.quota.UserResourceQuota;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.StorageEngineMessages;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CapacityResourceLimiterTest {

  @Before
  public void setUp() {
    IoTDBDescriptor.getInstance().getConfig().setQuotaEnable(true);
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setQuotaEnable(false);
  }

  @Test
  public void testMaxAndMinScheduling() {
    CapacityResourceLimiter limiter = new CapacityResourceLimiter();
    NodeQuotaState node = new NodeQuotaState(1, 4);
    node.updateRange("u1", new ResourceQuotaRange(1, 3));
    node.updateRange("u2", new ResourceQuotaRange(1, 2));

    Assert.assertTrue(
        limiter.tryAcquire("u1", 2, node.getRangeByUser().get("u1"), node).isSuccess());
    Assert.assertTrue(
        limiter.tryAcquire("u2", 2, node.getRangeByUser().get("u2"), node).isSuccess());
    LimiterAcquireResult denied =
        limiter.tryAcquire("u1", 1, node.getRangeByUser().get("u1"), node);
    Assert.assertFalse(denied.isSuccess());
    Assert.assertEquals(
        StorageEngineMessages.EXCEPTION_NODE_CAPACITY_EXCEEDED_89601D9A, denied.getRejectReason());

    limiter.release("u2", 1, node);
    Assert.assertTrue(
        limiter.tryAcquire("u1", 1, node.getRangeByUser().get("u1"), node).isSuccess());
  }

  @Test
  public void testRejectReasonUserMax() {
    CapacityResourceLimiter limiter = new CapacityResourceLimiter();
    NodeQuotaState node = new NodeQuotaState(1, 100);
    node.updateRange("u1", new ResourceQuotaRange(0, 2));
    Assert.assertTrue(
        limiter.tryAcquire("u1", 2, node.getRangeByUser().get("u1"), node).isSuccess());
    LimiterAcquireResult denied =
        limiter.tryAcquire("u1", 1, node.getRangeByUser().get("u1"), node);
    Assert.assertFalse(denied.isSuccess());
    Assert.assertEquals(
        StorageEngineMessages.EXCEPTION_USER_MAX_EXCEEDED_3D400C08, denied.getRejectReason());
  }

  @Test
  public void testRejectReasonMinGap() {
    CapacityResourceLimiter limiter = new CapacityResourceLimiter();
    NodeQuotaState node = new NodeQuotaState(1, 4);
    node.updateRange("core", new ResourceQuotaRange(3, 4));
    LimiterAcquireResult denied = limiter.tryAcquire("free", 2, null, node);
    Assert.assertFalse(denied.isSuccess());
    Assert.assertEquals(
        StorageEngineMessages.EXCEPTION_MIN_GAP_RESERVATION_B84C4EE4, denied.getRejectReason());
  }

  @Test
  public void testManagerAcquireRelease() {
    UserResourceQuotaManager manager = UserResourceQuotaManager.getInstance();
    UserResourceQuota quota = new UserResourceQuota();
    quota.getReadQuota().put(ResourceType.CPU, new ResourceQuotaRange(0, 2));
    manager.updateQuota("quotaTestUser", quota);

    AcquireContext ctx = new AcquireContext().setStatementType("QUERY");
    AcquirePolicy policy = AcquirePolicy.defaults();
    policy.setMaxWaitMs(0);
    QuotaToken token =
        manager
            .acquire("quotaTestUser", OperationType.READ, ResourceType.CPU, 1, ctx, policy)
            .getToken();
    Assert.assertEquals(1, manager.getInUse("quotaTestUser", OperationType.READ, ResourceType.CPU));
    token.close();
    Assert.assertEquals(0, manager.getInUse("quotaTestUser", OperationType.READ, ResourceType.CPU));
  }

  @Test
  public void testManagerRejectIncludesConcreteReason() {
    UserResourceQuotaManager manager = UserResourceQuotaManager.getInstance();
    UserResourceQuota quota = new UserResourceQuota();
    quota.getReadQuota().put(ResourceType.CPU, new ResourceQuotaRange(0, 1));
    manager.updateQuota("rejectReasonUser", quota);

    AcquirePolicy policy = AcquirePolicy.defaults();
    policy.setMaxWaitMs(0);
    Assert.assertTrue(
        manager
            .acquire(
                "rejectReasonUser",
                OperationType.READ,
                ResourceType.CPU,
                1,
                new AcquireContext(),
                policy)
            .isSuccess());
    AcquireResult rejected =
        manager.acquire(
            "rejectReasonUser",
            OperationType.READ,
            ResourceType.CPU,
            1,
            new AcquireContext(),
            policy);
    Assert.assertFalse(rejected.isSuccess());
    Assert.assertTrue(
        rejected
            .getRejectReason()
            .contains(StorageEngineMessages.EXCEPTION_USER_MAX_EXCEEDED_3D400C08));
  }

  @Test
  public void testUnlimitedUserPassesWhenCapacityAllows() {
    UserResourceQuotaManager manager = UserResourceQuotaManager.getInstance();
    AcquirePolicy policy = AcquirePolicy.defaults();
    policy.setMaxWaitMs(0);
    QuotaToken token =
        manager
            .acquire(
                "noQuotaUser",
                OperationType.READ,
                ResourceType.CPU,
                1,
                new AcquireContext(),
                policy)
            .getToken();
    Assert.assertNotNull(token);
    Assert.assertTrue(manager.getInUse("noQuotaUser", OperationType.READ, ResourceType.CPU) >= 1);
    token.close();
  }

  @Test
  public void testUnconfiguredUserRespectsMinGap() {
    CapacityResourceLimiter limiter = new CapacityResourceLimiter();
    NodeQuotaState node = new NodeQuotaState(1, 4);
    node.updateRange("core", new ResourceQuotaRange(3, 4));
    Assert.assertFalse(limiter.tryAcquire("free", 2, null, node).isSuccess());
    Assert.assertTrue(
        limiter.tryAcquire("core", 3, node.getRangeByUser().get("core"), node).isSuccess());
    Assert.assertTrue(limiter.tryAcquire("free", 1, null, node).isSuccess());
  }

  @Test
  public void testLowerMaxDoesNotKillExistingInUse() {
    CapacityResourceLimiter limiter = new CapacityResourceLimiter();
    NodeQuotaState node = new NodeQuotaState(1, 10);
    node.updateRange("u1", new ResourceQuotaRange(0, 8));
    Assert.assertTrue(
        limiter.tryAcquire("u1", 6, node.getRangeByUser().get("u1"), node).isSuccess());
    node.updateRange("u1", new ResourceQuotaRange(0, 4));
    Assert.assertEquals(6, node.inUse("u1"));
    LimiterAcquireResult denied =
        limiter.tryAcquire("u1", 1, node.getRangeByUser().get("u1"), node);
    Assert.assertFalse(denied.isSuccess());
    Assert.assertEquals(
        StorageEngineMessages.EXCEPTION_USER_MAX_EXCEEDED_3D400C08, denied.getRejectReason());
  }

  @Test
  public void testClearUserQuotaKeepsInUseForInFlightToken() {
    UserResourceQuotaManager manager = UserResourceQuotaManager.getInstance();
    UserResourceQuota quota = new UserResourceQuota();
    quota.getReadQuota().put(ResourceType.CPU, new ResourceQuotaRange(0, 4));
    manager.updateQuota("clearKeepUser", quota);

    AcquirePolicy policy = AcquirePolicy.defaults();
    policy.setMaxWaitMs(0);
    QuotaToken token =
        manager
            .acquire(
                "clearKeepUser",
                OperationType.READ,
                ResourceType.CPU,
                2,
                new AcquireContext(),
                policy)
            .getToken();
    Assert.assertEquals(2, manager.getInUse("clearKeepUser", OperationType.READ, ResourceType.CPU));

    manager.clearUserQuota("clearKeepUser");
    Assert.assertNull(manager.getUserQuota("clearKeepUser"));
    Assert.assertEquals(2, manager.getInUse("clearKeepUser", OperationType.READ, ResourceType.CPU));

    token.close();
    Assert.assertEquals(0, manager.getInUse("clearKeepUser", OperationType.READ, ResourceType.CPU));
  }

  @Test
  public void testIsClearRequest() {
    Assert.assertTrue(UserResourceQuotaManager.isClearRequest(null));
    Assert.assertTrue(UserResourceQuotaManager.isClearRequest(new TUserResourceQuota()));
    TUserResourceQuota quota = new TUserResourceQuota();
    quota.putToReadQuota(
        org.apache.iotdb.common.rpc.thrift.TResourceType.CPU,
        new org.apache.iotdb.common.rpc.thrift.TResourceQuotaRange(0, 1));
    Assert.assertFalse(UserResourceQuotaManager.isClearRequest(quota));
  }

  @Test
  public void testDefaultTempDiskBytesIsMinOfHundredGiBAndTenthDisk() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    long computed = config.computeDefaultTempDiskBytes();
    long hundredGiB = 100L * 1024 * 1024 * 1024;
    Assert.assertTrue(computed > 0);
    Assert.assertTrue(computed <= hundredGiB);
    long totalSpace = 0L;
    String[] dirs = config.getDataDirs();
    if (dirs != null) {
      for (String dir : dirs) {
        if (dir != null) {
          long space = new java.io.File(dir).getTotalSpace();
          if (space > 0) {
            totalSpace += space;
          }
        }
      }
    }
    if (totalSpace > 0) {
      Assert.assertEquals(Math.min(hundredGiB, totalSpace / 10), computed);
    } else {
      Assert.assertEquals(hundredGiB, computed);
    }
  }

  @Test
  public void testRootUserExempt() {
    UserResourceQuotaManager manager = UserResourceQuotaManager.getInstance();
    UserResourceQuota quota = new UserResourceQuota();
    quota.getReadQuota().put(ResourceType.CPU, new ResourceQuotaRange(0, 1));
    manager.updateQuota(IoTDBConstant.PATH_ROOT, quota);
    AcquirePolicy policy = AcquirePolicy.defaults();
    policy.setMaxWaitMs(0);
    Assert.assertTrue(
        manager
            .acquire(
                IoTDBConstant.PATH_ROOT,
                OperationType.READ,
                ResourceType.CPU,
                1,
                new AcquireContext(),
                policy)
            .isSuccess());
  }

  @Test
  public void testWriteTempDiskMaxRejectDoesNotAffectOtherUser() {
    UserResourceQuotaManager manager = UserResourceQuotaManager.getInstance();
    UserResourceQuota q1 = new UserResourceQuota();
    q1.getWriteQuota().put(ResourceType.TEMP_DISK, new ResourceQuotaRange(0, 1));
    manager.updateQuota("td_u1", q1);
    UserResourceQuota q2 = new UserResourceQuota();
    q2.getWriteQuota().put(ResourceType.TEMP_DISK, new ResourceQuotaRange(0, 10));
    manager.updateQuota("td_u2", q2);

    AcquirePolicy policy = AcquirePolicy.defaults();
    policy.setMaxWaitMs(0);
    Assert.assertFalse(
        manager
            .acquire(
                "td_u1",
                OperationType.WRITE,
                ResourceType.TEMP_DISK,
                2,
                new AcquireContext(),
                policy)
            .isSuccess());
    AcquireResult ok =
        manager.acquire(
            "td_u2", OperationType.WRITE, ResourceType.TEMP_DISK, 2, new AcquireContext(), policy);
    Assert.assertTrue(ok.isSuccess());
    ok.getToken().close();
  }
}
