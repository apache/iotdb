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

import org.apache.iotdb.common.rpc.thrift.TThrottleQuota;
import org.apache.iotdb.common.rpc.thrift.TTimedQuota;
import org.apache.iotdb.common.rpc.thrift.ThrottleType;
import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.schema.column.ColumnHeader;
import org.apache.iotdb.commons.schema.column.ColumnHeaderConstant;
import org.apache.iotdb.confignode.rpc.thrift.TThrottleQuotaResp;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeader;
import org.apache.iotdb.db.queryengine.common.header.DatasetHeaderFactory;
import org.apache.iotdb.db.queryengine.plan.execution.config.ConfigTaskResult;
import org.apache.iotdb.db.queryengine.plan.execution.config.IConfigTask;
import org.apache.iotdb.db.queryengine.plan.execution.config.executor.IConfigTaskExecutor;
import org.apache.iotdb.db.queryengine.plan.statement.sys.quota.ShowThrottleQuotaStatement;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.read.common.block.TsBlockBuilder;
import org.apache.tsfile.utils.BytesUtils;

import java.io.File;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ShowThrottleQuotaTask implements IConfigTask {

  private ShowThrottleQuotaStatement showThrottleQuotaStatement;

  public ShowThrottleQuotaTask(ShowThrottleQuotaStatement showThrottleQuotaStatement) {
    this.showThrottleQuotaStatement = showThrottleQuotaStatement;
  }

  @Override
  public ListenableFuture<ConfigTaskResult> execute(IConfigTaskExecutor configTaskExecutor)
      throws InterruptedException {
    return configTaskExecutor.showThrottleQuota(showThrottleQuotaStatement);
  }

  public static void buildTSBlock(
      TThrottleQuotaResp throttleQuotaResp, SettableFuture<ConfigTaskResult> future) {
    List<TSDataType> outputDataTypes =
        ColumnHeaderConstant.showThrottleQuotaColumnHeaders.stream()
            .map(ColumnHeader::getColumnType)
            .collect(Collectors.toList());
    TsBlockBuilder builder = new TsBlockBuilder(outputDataTypes);
    if (throttleQuotaResp.getThrottleQuota() != null) {
      for (Map.Entry<String, TThrottleQuota> throttleQuota :
          throttleQuotaResp.getThrottleQuota().entrySet()) {
        for (Map.Entry<ThrottleType, TTimedQuota> entry :
            throttleQuota.getValue().getThrottleLimit().entrySet()) {
          builder.getTimeColumnBuilder().writeLong(0L);
          builder.getColumnBuilder(0).writeBinary(BytesUtils.valueOf(throttleQuota.getKey()));
          builder
              .getColumnBuilder(1)
              .writeBinary(BytesUtils.valueOf(toThrottleType(entry.getKey())));
          builder
              .getColumnBuilder(2)
              .writeBinary(BytesUtils.valueOf(toQuotaLimit(entry.getKey(), entry.getValue())));
          builder
              .getColumnBuilder(3)
              .writeBinary(BytesUtils.valueOf(toRequestType(entry.getKey())));
          builder.declarePosition();
        }
        if (throttleQuota.getValue().getMemLimit() != 0) {
          builder.getTimeColumnBuilder().writeLong(0L);
          builder.getColumnBuilder(0).writeBinary(BytesUtils.valueOf(throttleQuota.getKey()));
          builder
              .getColumnBuilder(1)
              .writeBinary(BytesUtils.valueOf(IoTDBConstant.MEMORY_SIZE_PER_READ));
          builder
              .getColumnBuilder(2)
              .writeBinary(
                  BytesUtils.valueOf(
                      throttleQuota.getValue().getMemLimit() / IoTDBConstant.KB / IoTDBConstant.KB
                          + IoTDBConstant.MB_UNIT));
          builder
              .getColumnBuilder(3)
              .writeBinary(BytesUtils.valueOf(IoTDBConstant.REQUEST_TYPE_READ));
          builder.declarePosition();
        }

        if (throttleQuota.getValue().getCpuLimit() != 0) {
          builder.getTimeColumnBuilder().writeLong(0L);
          builder.getColumnBuilder(0).writeBinary(BytesUtils.valueOf(throttleQuota.getKey()));
          builder
              .getColumnBuilder(1)
              .writeBinary(BytesUtils.valueOf(IoTDBConstant.CPU_NUMBER_PER_READ));
          builder
              .getColumnBuilder(2)
              .writeBinary(BytesUtils.valueOf(throttleQuota.getValue().getCpuLimit() + ""));
          builder
              .getColumnBuilder(3)
              .writeBinary(BytesUtils.valueOf(IoTDBConstant.REQUEST_TYPE_READ));
          builder.declarePosition();
        }
      }
    }
    DatasetHeader datasetHeader = DatasetHeaderFactory.getShowThrottleQuotaHeader();
    future.set(new ConfigTaskResult(TSStatusCode.SUCCESS_STATUS, builder.build(), datasetHeader));
  }

  private static String toThrottleType(ThrottleType throttleType) {
    switch (throttleType) {
      case REQUEST_SIZE:
      case WRITE_SIZE:
      case READ_SIZE:
        return IoTDBConstant.REQUEST_SIZE_PER_UNIT_TIME;
      case REQUEST_NUMBER:
      case READ_NUMBER:
      case WRITE_NUMBER:
        return IoTDBConstant.REQUEST_NUM_PER_UNIT_TIME;
      default:
        return "";
    }
  }

  private static String toRequestType(ThrottleType throttleType) {
    switch (throttleType) {
      case WRITE_NUMBER:
      case WRITE_SIZE:
        return IoTDBConstant.REQUEST_TYPE_WRITE;
      case READ_NUMBER:
      case READ_SIZE:
        return IoTDBConstant.REQUEST_TYPE_READ;
      case REQUEST_NUMBER:
      case REQUEST_SIZE:
        return "";
      default:
        throw new IllegalArgumentException("Wrong request type");
    }
  }

  private static String toQuotaLimit(ThrottleType throttleType, TTimedQuota timedQuota) {
    switch (toThrottleType(throttleType)) {
      case IoTDBConstant.REQUEST_NUM_PER_UNIT_TIME:
        return timedQuota.getSoftLimit()
            + IoTDBConstant.REQ_UNIT
            + File.separator
            + toTimeUnit(timedQuota.getTimeUnit());
      case IoTDBConstant.REQUEST_SIZE_PER_UNIT_TIME:
        if (timedQuota.getSoftLimit() < IoTDBConstant.KB) {
          return timedQuota.getSoftLimit()
              + IoTDBConstant.B_UNIT
              + File.separator
              + toTimeUnit(timedQuota.getTimeUnit());
        } else if (timedQuota.getSoftLimit() < IoTDBConstant.MB) {
          return timedQuota.getSoftLimit() / IoTDBConstant.KB
              + IoTDBConstant.KB_UNIT
              + File.separator
              + toTimeUnit(timedQuota.getTimeUnit());
        } else {
          return timedQuota.getSoftLimit() / IoTDBConstant.KB / IoTDBConstant.KB
              + IoTDBConstant.MB_UNIT
              + File.separator
              + toTimeUnit(timedQuota.getTimeUnit());
        }
      default:
        throw new IllegalArgumentException("Wrong request type");
    }
  }

  private static String toTimeUnit(long timeUnit) {
    switch ((int) timeUnit) {
      case IoTDBConstant.SEC:
        return IoTDBConstant.SEC_UNIT;
      case IoTDBConstant.MIN:
        return IoTDBConstant.MIN_UNIT;
      case IoTDBConstant.HOUR:
        return IoTDBConstant.HOUR_UNIT;
      case IoTDBConstant.DAY:
        return IoTDBConstant.DAY_UNIT;
      default:
        throw new IllegalArgumentException("Wrong unit type");
    }
  }
}
