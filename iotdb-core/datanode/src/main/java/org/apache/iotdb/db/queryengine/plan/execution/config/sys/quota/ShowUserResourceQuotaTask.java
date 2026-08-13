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

package org.apache.iotdb.db.queryengine.plan.execution.config.sys.quota;

import org.apache.iotdb.common.rpc.thrift.TResourceQuotaRange;
import org.apache.iotdb.common.rpc.thrift.TResourceType;
import org.apache.iotdb.common.rpc.thrift.TTimedQuota;
import org.apache.iotdb.common.rpc.thrift.TUserResourceQuota;
import org.apache.iotdb.common.rpc.thrift.TUserResourceUsageSnapshot;
import org.apache.iotdb.common.rpc.thrift.ThrottleType;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TUserResourceQuotaResp;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.ShowUserResourceQuotaStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.BytesUtils;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.stream.Collectors;

/**
 * SHOW USER QUOTA: one row set per alive DataNode (usage from CN reportUserResourceUsage cache),
 * NodeID = DataNode id.
 */
public class ShowUserResourceQuotaTask implements IConfigTask {

  private final ShowUserResourceQuotaStatement statement;

  public ShowUserResourceQuotaTask(ShowUserResourceQuotaStatement statement) {
    this.statement = statement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showUserResourceQuota(statement);
  }

  /** Flatten per-user quotas and per-DataNode in-use into SHOW USER QUOTA rows. */
  public static void buildTSBlock(
      TUserResourceQuotaResp resp, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> types =
        ColumnHeaderConstant.showUserResourceQuotaColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(types);

    TreeSet<Integer> nodeIds = new TreeSet<>();
    Map<Integer, TUserResourceUsageSnapshot> usageByNode =
        resp.isSetUsageByDataNode() && resp.getUsageByDataNode() != null
            ? resp.getUsageByDataNode()
            : Collections.emptyMap();
    nodeIds.addAll(usageByNode.keySet());
    if (nodeIds.isEmpty()) {
      // CN did not return usageByDataNode (e.g. no Running DN visible yet): fall back so SHOW still
      // returns quota rows. Normal path always lists all Running DNs from CN.
      nodeIds.add(IoTDBDescriptor.getInstance().getConfig().getDataNodeId());
    }

    if (resp.getUserResourceQuota() != null) {
      Map<Long, String> nameMap =
          resp.isSetUserNameMap() && resp.getUserNameMap() != null
              ? resp.getUserNameMap()
              : Collections.emptyMap();
      for (Integer nodeId : nodeIds) {
        String nodeIdText = String.valueOf(nodeId);
        TUserResourceUsageSnapshot snap = usageByNode.get(nodeId);
        for (Map.Entry<Long, TUserResourceQuota> entry : resp.getUserResourceQuota().entrySet()) {
          long userId = entry.getKey();
          String displayName = nameMap.getOrDefault(userId, String.valueOf(userId));
          appendRanges(
              builder,
              userId,
              displayName,
              nodeIdText,
              IoTDBConstant.REQUEST_TYPE_READ,
              entry.getValue().getReadQuota(),
              snap == null ? null : snap.getReadInUse());
          appendRanges(
              builder,
              userId,
              displayName,
              nodeIdText,
              IoTDBConstant.REQUEST_TYPE_WRITE,
              entry.getValue().getWriteQuota(),
              snap == null ? null : snap.getWriteInUse());
          appendDiskIo(builder, displayName, nodeIdText, entry.getValue().getThrottleLimit());
        }
      }
    }
    DatasetHeader header = DatasetHeaderFactory.getShowUserResourceQuotaHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), header));
  }

  /** One SHOW row per resource range; Used/MinGap come from the DN usage snapshot. */
  private static void appendRanges(
      TsBlockBuilder builder,
      long userId,
      String displayName,
      String nodeIdText,
      String opLabel,
      Map<TResourceType, TResourceQuotaRange> ranges,
      Map<Long, Map<TResourceType, Long>> inUseByUser) {
    if (ranges == null) {
      return;
    }
    for (Map.Entry<TResourceType, TResourceQuotaRange> entry : ranges.entrySet()) {
      long used = 0L;
      if (inUseByUser != null && inUseByUser.containsKey(userId)) {
        Long v = inUseByUser.get(userId).get(entry.getKey());
        if (v != null) {
          used = v;
        }
      }
      long min = entry.getValue().getMinValue();
      long minGap =
          (min == IoTDBConstant.UNLIMITED_VALUE || min < 0) ? 0L : Math.max(0L, min - used);
      appendRow(
          builder,
          displayName,
          nodeIdText,
          opLabel,
          entry.getKey().name().toLowerCase(),
          format(min),
          format(entry.getValue().getMaxValue()),
          String.valueOf(used),
          formatGap(min, minGap));
    }
  }

  private static void appendDiskIo(
      TsBlockBuilder builder,
      String user,
      String nodeIdText,
      Map<ThrottleType, TTimedQuota> throttleLimit) {
    if (throttleLimit == null) {
      return;
    }
    appendDiskIoSide(
        builder,
        user,
        nodeIdText,
        IoTDBConstant.REQUEST_TYPE_READ,
        throttleLimit.get(ThrottleType.READ_SIZE));
    appendDiskIoSide(
        builder,
        user,
        nodeIdText,
        IoTDBConstant.REQUEST_TYPE_WRITE,
        throttleLimit.get(ThrottleType.WRITE_SIZE));
  }

  private static void appendDiskIoSide(
      TsBlockBuilder builder,
      String user,
      String nodeIdText,
      String opLabel,
      TTimedQuota timedQuota) {
    if (timedQuota == null) {
      return;
    }
    appendRow(
        builder,
        user,
        nodeIdText,
        opLabel,
        "disk_io",
        "-",
        formatDiskIoLimit(timedQuota),
        "-",
        "-");
  }

  private static void appendRow(
      TsBlockBuilder builder,
      String user,
      String nodeIdText,
      String opLabel,
      String quotaType,
      String min,
      String max,
      String used,
      String minGap) {
    builder.getTimeColumnBuilder().writeLong(0L);
    builder.getColumnBuilder(0).writeBinary(BytesUtils.valueOf(user));
    builder.getColumnBuilder(1).writeBinary(BytesUtils.valueOf(nodeIdText));
    builder.getColumnBuilder(2).writeBinary(BytesUtils.valueOf(opLabel));
    builder.getColumnBuilder(3).writeBinary(BytesUtils.valueOf(quotaType));
    builder.getColumnBuilder(4).writeBinary(BytesUtils.valueOf(min));
    builder.getColumnBuilder(5).writeBinary(BytesUtils.valueOf(max));
    builder.getColumnBuilder(6).writeBinary(BytesUtils.valueOf(used));
    builder.getColumnBuilder(7).writeBinary(BytesUtils.valueOf(minGap));
    builder.declarePosition();
  }

  private static String format(long value) {
    return value == IoTDBConstant.UNLIMITED_VALUE ? "-" : String.valueOf(value);
  }

  private static String formatGap(long minValue, long minGap) {
    if (minValue == IoTDBConstant.UNLIMITED_VALUE || minValue < 0) {
      return "-";
    }
    return String.valueOf(minGap);
  }

  private static String formatDiskIoLimit(TTimedQuota timedQuota) {
    // USER QUOTA disk_io uses fixed unit bytes/sec stored as softLimit with timeUnit=SEC.
    long bytesPerSec =
        timedQuota.getTimeUnit() <= 0
            ? timedQuota.getSoftLimit()
            : timedQuota.getSoftLimit() * IoTDBConstant.SEC / timedQuota.getTimeUnit();
    return String.valueOf(bytesPerSec);
  }
}
