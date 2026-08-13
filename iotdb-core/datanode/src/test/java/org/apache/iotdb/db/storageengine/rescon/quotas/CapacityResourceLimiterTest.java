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
import org.apache.iotdb.commons.quota.UserResourceQuotaConverter;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.i18n.StorageEngineMessages;
import org.apache.iotdb.db.queryengine.plan.statement.crud.InsertRowStatement;
import org.apache.iotdb.db.utils.memory.WriteMemoryEstimator;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class CapacityResourceLimiterTest {

  private static final long U1 = 1001L;
  private static final long U2 = 1002L;
  private static final long CORE_USER = 1003L;
  private static final long FREE_USER = 1004L;
  private static final long QUOTA_TEST_USER = 2001L;
  private static final long RW_USER = 2002L;
  private static final long REJECT_REASON_USER = 2003L;
  private static final long NO_QUOTA_USER = 2004L;
  private static final long CLEAR_KEEP_USER = 2005L;
  private static final long TD_U1 = 2006L;
  private static final long TD_U2 = 2007L;
  private static final long SNAP_USER = 2008L;

  @Before
  public void setUp() {
    IoTDBDescriptor.getInstance().getConfig().setQuotaEnable(true);
  }

  @After
  public void tearDown() {
    IoTDBDescriptor.getInstance().getConfig().setQuotaEnable(false);
  }

  @Test
  public void testNodeCapacityHotReloadAffectsAcquire() {
    CapacityResourceLimiter limiter = new CapacityResourceLimiter();
    NodeQuotaState node = new NodeQuotaState(1, 4);
    ResourceQuotaRange range = new ResourceQuotaRange(0, 10);

    Assert.assertTrue(limiter.tryAcquire(U1, OperationType.READ, 3, range, node).isSuccess());
    Assert.assertFalse(limiter.tryAcquire(U2, OperationType.READ, 2, range, node).isSuccess());

    node.setNodeCapacity(10);
    Assert.assertTrue(limiter.tryAcquire(U2, OperationType.READ, 2, range, node).isSuccess());

    node.setNodeCapacity(4);
    // Already using 5 (> 4); new acquire still rejected
    Assert.assertFalse(limiter.tryAcquire(U2, OperationType.READ, 1, range, node).isSuccess());
  }

  @Test
  public void testMaxAndMinScheduling() {
    CapacityResourceLimiter limiter = new CapacityResourceLimiter();
    NodeQuotaState node = new NodeQuotaState(1, 4);
    node.updateRange(OperationType.READ, U1, new ResourceQuotaRange(1, 3));
    node.updateRange(OperationType.READ, U2, new ResourceQuotaRange(1, 2));

    Assert.assertTrue(
        limiter
            .tryAcquire(
                U1, OperationType.READ, 2, node.getRangeByUser(OperationType.READ).get(U1), node)
            .isSuccess());
    Assert.assertTrue(
        limiter
            .tryAcquire(
                U2, OperationType.READ, 2, node.getRangeByUser(OperationType.READ).get(U2), node)
            .isSuccess());
    LimiterAcquireResult denied =
        limiter.tryAcquire(
            U1, OperationType.READ, 1, node.getRangeByUser(OperationType.READ).get(U1), node);
    Assert.assertFalse(denied.isSuccess());
    Assert.assertEquals(
        String.format(
            StorageEngineMessages.EXCEPTION_NODE_CAPACITY_EXCEEDED_FOR_ARG_7D36CEB7, "resource"),
        denied.getRejectReason());

    limiter.release(U2, OperationType.READ, 1, node);
    Assert.assertTrue(
        limiter
            .tryAcquire(
                U1, OperationType.READ, 1, node.getRangeByUser(OperationType.READ).get(U1), node)
            .isSuccess());
  }

  @Test
  public void testRejectReasonUserMax() {
    CapacityResourceLimiter limiter = new CapacityResourceLimiter();
    NodeQuotaState node = new NodeQuotaState(1, 100);
    node.updateRange(OperationType.READ, U1, new ResourceQuotaRange(0, 2));
    Assert.assertTrue(
        limiter
            .tryAcquire(
                U1, OperationType.READ, 2, node.getRangeByUser(OperationType.READ).get(U1), node)
            .isSuccess());
    LimiterAcquireResult denied =
        limiter.tryAcquire(
            U1, OperationType.READ, 1, node.getRangeByUser(OperationType.READ).get(U1), node);
    Assert.assertFalse(denied.isSuccess());
    Assert.assertEquals(
        String.format(
            StorageEngineMessages.EXCEPTION_USER_MAX_EXCEEDED_FOR_ARG_F411FFB3, "resource"),
        denied.getRejectReason());
  }

  @Test
  public void testRejectReasonMinGap() {
    CapacityResourceLimiter limiter = new CapacityResourceLimiter();
    NodeQuotaState node = new NodeQuotaState(1, 4);
    node.updateRange(OperationType.READ, CORE_USER, new ResourceQuotaRange(3, 4));
    LimiterAcquireResult denied = limiter.tryAcquire(FREE_USER, OperationType.READ, 2, null, node);
    Assert.assertFalse(denied.isSuccess());
    Assert.assertEquals(
        String.format(
            StorageEngineMessages.EXCEPTION_MIN_GAP_RESERVATION_FOR_ARG_6F3E0949, "resource"),
        denied.getRejectReason());
  }

  @Test
  public void testReadWriteMaxAreIndependent() {
    CapacityResourceLimiter limiter = new CapacityResourceLimiter();
    NodeQuotaState node = new NodeQuotaState(1, 100);
    node.updateRange(OperationType.READ, U1, new ResourceQuotaRange(0, 2));
    node.updateRange(OperationType.WRITE, U1, new ResourceQuotaRange(0, 5));

    Assert.assertTrue(
        limiter
            .tryAcquire(
                U1, OperationType.READ, 2, node.getRangeByUser(OperationType.READ).get(U1), node)
            .isSuccess());
    // Read at max must not block write against its own max.
    Assert.assertTrue(
        limiter
            .tryAcquire(
                U1, OperationType.WRITE, 5, node.getRangeByUser(OperationType.WRITE).get(U1), node)
            .isSuccess());
    Assert.assertEquals(2, node.inUse(OperationType.READ, U1));
    Assert.assertEquals(5, node.inUse(OperationType.WRITE, U1));
  }

  @Test
  public void testManagerAcquireRelease() {
    UserResourceQuotaManager manager = UserResourceQuotaManager.getInstance();
    UserResourceQuota quota = new UserResourceQuota();
    quota.getReadQuota().put(ResourceType.CPU, new ResourceQuotaRange(0, 2));
    manager.updateQuota(QUOTA_TEST_USER, "quotaTestUser", quota);

    AcquireContext ctx = new AcquireContext().setStatementType("QUERY");
    AcquirePolicy policy = AcquirePolicy.defaults();
    policy.setMaxWaitMs(0);
    QuotaToken token =
        manager
            .acquire(QUOTA_TEST_USER, OperationType.READ, ResourceType.CPU, 1, ctx, policy)
            .getToken();
    Assert.assertEquals(1, manager.getInUse(QUOTA_TEST_USER, OperationType.READ, ResourceType.CPU));
    token.close();
    Assert.assertEquals(0, manager.getInUse(QUOTA_TEST_USER, OperationType.READ, ResourceType.CPU));
  }

  @Test
  public void testManagerReadWriteRangesDoNotOverwrite() {
    UserResourceQuotaManager manager = UserResourceQuotaManager.getInstance();
    UserResourceQuota quota = new UserResourceQuota();
    quota.getReadQuota().put(ResourceType.CPU, new ResourceQuotaRange(2, 4));
    quota.getWriteQuota().put(ResourceType.CPU, new ResourceQuotaRange(1, 3));
    manager.updateQuota(RW_USER, "rwUser", quota);

    NodeQuotaState cpu = manager.getNodeState(ResourceType.CPU);
    Assert.assertEquals(2, cpu.min(OperationType.READ, RW_USER));
    Assert.assertEquals(1, cpu.min(OperationType.WRITE, RW_USER));
    Assert.assertEquals(4, cpu.getRangeByUser(OperationType.READ).get(RW_USER).getMaxValue());
    Assert.assertEquals(3, cpu.getRangeByUser(OperationType.WRITE).get(RW_USER).getMaxValue());
  }

  @Test
  public void testManagerRejectIncludesConcreteReason() {
    UserResourceQuotaManager manager = UserResourceQuotaManager.getInstance();
    UserResourceQuota quota = new UserResourceQuota();
    quota.getReadQuota().put(ResourceType.CPU, new ResourceQuotaRange(0, 1));
    manager.updateQuota(REJECT_REASON_USER, "rejectReasonUser", quota);

    AcquirePolicy policy = AcquirePolicy.defaults();
    policy.setMaxWaitMs(0);
    Assert.assertTrue(
        manager
            .acquire(
                REJECT_REASON_USER,
                OperationType.READ,
                ResourceType.CPU,
                1,
                new AcquireContext(),
                policy)
            .isSuccess());
    AcquireResult rejected =
        manager.acquire(
            REJECT_REASON_USER,
            OperationType.READ,
            ResourceType.CPU,
            1,
            new AcquireContext(),
            policy);
    Assert.assertFalse(rejected.isSuccess());
    Assert.assertTrue(
        rejected
            .getRejectReason()
            .contains(
                String.format(
                    StorageEngineMessages.EXCEPTION_USER_MAX_EXCEEDED_FOR_ARG_F411FFB3, "cpu")));
  }

  @Test
  public void testMinBypassRejectsAmountExceedingMinHeadroom() {
    CapacityResourceLimiter limiter = new CpuSlotLimiter();
    NodeQuotaState node = new NodeQuotaState(1, 100);
    node.updateRange(OperationType.READ, U1, new ResourceQuotaRange(100, 200));

    Assert.assertTrue(
        limiter
            .tryAcquire(
                U1, OperationType.READ, 1, node.getRangeByUser(OperationType.READ).get(U1), node)
            .isSuccess());
    LimiterAcquireResult denied =
        limiter.tryAcquire(
            U1, OperationType.READ, 10000, node.getRangeByUser(OperationType.READ).get(U1), node);
    Assert.assertFalse(denied.isSuccess());
    Assert.assertEquals(
        String.format(StorageEngineMessages.EXCEPTION_USER_MAX_EXCEEDED_FOR_ARG_F411FFB3, "cpu"),
        denied.getRejectReason());
  }

  @Test
  public void testUnlimitedUserPassesWhenCapacityAllows() {
    UserResourceQuotaManager manager = UserResourceQuotaManager.getInstance();
    AcquirePolicy policy = AcquirePolicy.defaults();
    policy.setMaxWaitMs(0);
    QuotaToken token =
        manager
            .acquire(
                NO_QUOTA_USER,
                OperationType.READ,
                ResourceType.CPU,
                1,
                new AcquireContext(),
                policy)
            .getToken();
    Assert.assertNotNull(token);
    Assert.assertTrue(manager.getInUse(NO_QUOTA_USER, OperationType.READ, ResourceType.CPU) >= 1);
    token.close();
  }

  @Test
  public void testUnconfiguredUserRespectsMinGap() {
    CapacityResourceLimiter limiter = new CapacityResourceLimiter();
    NodeQuotaState node = new NodeQuotaState(1, 4);
    node.updateRange(OperationType.READ, CORE_USER, new ResourceQuotaRange(3, 4));
    Assert.assertFalse(
        limiter.tryAcquire(FREE_USER, OperationType.READ, 2, null, node).isSuccess());
    Assert.assertTrue(
        limiter
            .tryAcquire(
                CORE_USER,
                OperationType.READ,
                3,
                node.getRangeByUser(OperationType.READ).get(CORE_USER),
                node)
            .isSuccess());
    Assert.assertTrue(limiter.tryAcquire(FREE_USER, OperationType.READ, 1, null, node).isSuccess());
  }

  @Test
  public void testLowerMaxDoesNotKillExistingInUse() {
    CapacityResourceLimiter limiter = new CapacityResourceLimiter();
    NodeQuotaState node = new NodeQuotaState(1, 10);
    node.updateRange(OperationType.READ, U1, new ResourceQuotaRange(0, 8));
    Assert.assertTrue(
        limiter
            .tryAcquire(
                U1, OperationType.READ, 6, node.getRangeByUser(OperationType.READ).get(U1), node)
            .isSuccess());
    node.updateRange(OperationType.READ, U1, new ResourceQuotaRange(0, 4));
    Assert.assertEquals(6, node.inUse(OperationType.READ, U1));
    LimiterAcquireResult denied =
        limiter.tryAcquire(
            U1, OperationType.READ, 1, node.getRangeByUser(OperationType.READ).get(U1), node);
    Assert.assertFalse(denied.isSuccess());
    Assert.assertEquals(
        String.format(
            StorageEngineMessages.EXCEPTION_USER_MAX_EXCEEDED_FOR_ARG_F411FFB3, "resource"),
        denied.getRejectReason());
  }

  @Test
  public void testClearUserQuotaKeepsInUseForInFlightToken() {
    UserResourceQuotaManager manager = UserResourceQuotaManager.getInstance();
    UserResourceQuota quota = new UserResourceQuota();
    quota.getReadQuota().put(ResourceType.CPU, new ResourceQuotaRange(0, 4));
    manager.updateQuota(CLEAR_KEEP_USER, "clearKeepUser", quota);

    AcquirePolicy policy = AcquirePolicy.defaults();
    policy.setMaxWaitMs(0);
    QuotaToken token =
        manager
            .acquire(
                CLEAR_KEEP_USER,
                OperationType.READ,
                ResourceType.CPU,
                2,
                new AcquireContext(),
                policy)
            .getToken();
    Assert.assertEquals(2, manager.getInUse(CLEAR_KEEP_USER, OperationType.READ, ResourceType.CPU));

    manager.clearUserQuota(CLEAR_KEEP_USER, "clearKeepUser");
    Assert.assertNull(manager.getUserQuota(CLEAR_KEEP_USER));
    Assert.assertEquals(2, manager.getInUse(CLEAR_KEEP_USER, OperationType.READ, ResourceType.CPU));

    token.close();
    Assert.assertEquals(0, manager.getInUse(CLEAR_KEEP_USER, OperationType.READ, ResourceType.CPU));
  }

  @Test
  public void testIsClearRequest() {
    Assert.assertTrue(UserResourceQuotaManager.isClearRequest(null));
    Assert.assertTrue(UserResourceQuotaConverter.isClearRequest(new TUserResourceQuota()));
    TUserResourceQuota quota = new TUserResourceQuota();
    quota.putToReadQuota(
        org.apache.iotdb.common.rpc.thrift.TResourceType.CPU,
        new org.apache.iotdb.common.rpc.thrift.TResourceQuotaRange(0, 1));
    Assert.assertFalse(UserResourceQuotaConverter.isClearRequest(quota));
  }

  @Test
  public void testUserResourceQuotaManagerIsClearRequestDelegates() {
    Assert.assertTrue(UserResourceQuotaManager.isClearRequest(new TUserResourceQuota()));
  }

  @Test
  public void testSyncQuotasFromConfigNode() {
    UserResourceQuotaManager manager = UserResourceQuotaManager.getInstance();
    final long keepUser = 2101L;
    final long dropUser = 2102L;
    final long addUser = 2103L;

    UserResourceQuota localKeep = new UserResourceQuota();
    localKeep.getReadQuota().put(ResourceType.CPU, new ResourceQuotaRange(0, 2));
    manager.updateQuota(keepUser, "keepUser", localKeep);
    UserResourceQuota localDrop = new UserResourceQuota();
    localDrop.getReadQuota().put(ResourceType.CPU, new ResourceQuotaRange(0, 3));
    manager.updateQuota(dropUser, "dropUser", localDrop);

    TUserResourceQuota remoteKeep = new TUserResourceQuota();
    remoteKeep.putToReadQuota(
        org.apache.iotdb.common.rpc.thrift.TResourceType.CPU,
        new org.apache.iotdb.common.rpc.thrift.TResourceQuotaRange(1, 5));
    TUserResourceQuota remoteAdd = new TUserResourceQuota();
    remoteAdd.putToReadQuota(
        org.apache.iotdb.common.rpc.thrift.TResourceType.MEMORY,
        new org.apache.iotdb.common.rpc.thrift.TResourceQuotaRange(0, 1024));

    java.util.Map<Long, TUserResourceQuota> remote = new java.util.HashMap<>();
    remote.put(keepUser, remoteKeep);
    remote.put(addUser, remoteAdd);
    java.util.Map<Long, String> names = new java.util.HashMap<>();
    names.put(keepUser, "keepUser");
    names.put(addUser, "addUser");

    manager.syncQuotasFromConfigNode(remote, names);

    Assert.assertNull(manager.getUserQuota(dropUser));
    Assert.assertNotNull(manager.getUserQuota(keepUser));
    Assert.assertEquals(
        5, manager.getUserQuota(keepUser).getReadQuota().get(ResourceType.CPU).getMaxValue());
    Assert.assertNotNull(manager.getUserQuota(addUser));
    Assert.assertEquals(
        1024, manager.getUserQuota(addUser).getReadQuota().get(ResourceType.MEMORY).getMaxValue());

    // Idempotent: second sync with same snapshot should not clear existing entries.
    manager.syncQuotasFromConfigNode(remote, names);
    Assert.assertNotNull(manager.getUserQuota(keepUser));
    Assert.assertNotNull(manager.getUserQuota(addUser));

    manager.clearUserQuota(keepUser, "keepUser");
    manager.clearUserQuota(addUser, "addUser");
  }

  @Test
  public void testDefaultTempDiskBytesIsMinOfHundredGiBAndTenthDisk() {
    IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
    long computed = config.computeDefaultTempDiskBytes();
    final long hundredGiB = 100L * 1024 * 1024 * 1024;
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
    manager.updateQuota(IoTDBConstant.SUPER_USER_ID, IoTDBConstant.PATH_ROOT, quota);
    AcquirePolicy policy = AcquirePolicy.defaults();
    policy.setMaxWaitMs(0);
    Assert.assertTrue(
        manager
            .acquire(
                IoTDBConstant.SUPER_USER_ID,
                OperationType.READ,
                ResourceType.CPU,
                1,
                new AcquireContext(),
                policy)
            .isSuccess());
  }

  @Test
  public void testReadTempDiskMaxRejectDoesNotAffectOtherUser() {
    UserResourceQuotaManager manager = UserResourceQuotaManager.getInstance();
    UserResourceQuota q1 = new UserResourceQuota();
    // Spill charges READ TEMP_DISK (query external sort), not write_temp_disk_*.
    q1.getReadQuota().put(ResourceType.TEMP_DISK, new ResourceQuotaRange(0, 1));
    manager.updateQuota(TD_U1, "td_u1", q1);
    UserResourceQuota q2 = new UserResourceQuota();
    q2.getReadQuota().put(ResourceType.TEMP_DISK, new ResourceQuotaRange(0, 10));
    manager.updateQuota(TD_U2, "td_u2", q2);

    AcquirePolicy policy = AcquirePolicy.defaults();
    policy.setMaxWaitMs(0);
    Assert.assertFalse(
        manager
            .acquire(
                TD_U1, OperationType.READ, ResourceType.TEMP_DISK, 2, new AcquireContext(), policy)
            .isSuccess());
    AcquireResult ok =
        manager.acquire(
            TD_U2, OperationType.READ, ResourceType.TEMP_DISK, 2, new AcquireContext(), policy);
    Assert.assertTrue(ok.isSuccess());
    ok.getToken().close();
  }

  @Test
  public void testSnapshotUsageSeparatesReadAndWrite() {
    UserResourceQuotaManager manager = UserResourceQuotaManager.getInstance();
    UserResourceQuota quota = new UserResourceQuota();
    quota.getReadQuota().put(ResourceType.CPU, new ResourceQuotaRange(0, 10));
    quota.getWriteQuota().put(ResourceType.CPU, new ResourceQuotaRange(0, 10));
    manager.updateQuota(SNAP_USER, "snapUser", quota);

    AcquirePolicy policy = AcquirePolicy.defaults();
    policy.setMaxWaitMs(0);
    QuotaToken readToken =
        manager
            .acquire(
                SNAP_USER, OperationType.READ, ResourceType.CPU, 2, new AcquireContext(), policy)
            .getToken();
    QuotaToken writeToken =
        manager
            .acquire(
                SNAP_USER, OperationType.WRITE, ResourceType.CPU, 3, new AcquireContext(), policy)
            .getToken();

    org.apache.iotdb.common.rpc.thrift.TUserResourceUsageSnapshot snap = manager.snapshotUsage();
    Assert.assertEquals(
        Long.valueOf(2L),
        snap.getReadInUse()
            .get(SNAP_USER)
            .get(org.apache.iotdb.common.rpc.thrift.TResourceType.CPU));
    Assert.assertEquals(
        Long.valueOf(3L),
        snap.getWriteInUse()
            .get(SNAP_USER)
            .get(org.apache.iotdb.common.rpc.thrift.TResourceType.CPU));

    readToken.close();
    writeToken.close();
  }

  @Test
  public void testWriteMemoryEstimatorUsesActualVariableWidthValueSize() {
    String value = "x".repeat(4096);
    InsertRowStatement statement = new InsertRowStatement();
    statement.setValues(new Object[] {value});

    Assert.assertEquals(
        value.getBytes(org.apache.tsfile.common.conf.TSFileConfig.STRING_CHARSET).length,
        WriteMemoryEstimator.estimate(statement));
  }
}
