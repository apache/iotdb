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

package org.apache.iotdb.db.queryengine.plan.statement.sys.quota;

import org.apache.iotdb.common.rpc.thrift.TResourceQuotaRange;
import org.apache.iotdb.common.rpc.thrift.TResourceType;
import org.apache.iotdb.common.rpc.thrift.TTimedQuota;
import org.apache.iotdb.common.rpc.thrift.TUserResourceQuota;
import org.apache.iotdb.common.rpc.thrift.ThrottleType;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import java.util.Collections;
import java.util.List;

public class SetUserResourceQuotaStatement extends Statement implements IConfigStatement {

  private String userName;
  private TUserResourceQuota userResourceQuota = new TUserResourceQuota();

  public SetUserResourceQuotaStatement() {
    statementType = StatementType.SET_USER_RESOURCE_QUOTA;
  }

  public String getUserName() {
    return userName;
  }

  public void setUserName(String userName) {
    this.userName = userName;
  }

  public TUserResourceQuota getUserResourceQuota() {
    return userResourceQuota;
  }

  public void setUserResourceQuota(TUserResourceQuota userResourceQuota) {
    this.userResourceQuota = userResourceQuota;
  }

  public void putRange(OperationSide side, ResourceSide resource, long min, long max) {
    TResourceType type = TResourceType.valueOf(resource.name());
    java.util.Map<TResourceType, TResourceQuotaRange> target;
    if (side == OperationSide.READ) {
      if (!userResourceQuota.isSetReadQuota()) {
        userResourceQuota.setReadQuota(new java.util.EnumMap<>(TResourceType.class));
      }
      target = userResourceQuota.getReadQuota();
    } else {
      if (!userResourceQuota.isSetWriteQuota()) {
        userResourceQuota.setWriteQuota(new java.util.EnumMap<>(TResourceType.class));
      }
      target = userResourceQuota.getWriteQuota();
    }
    // Merge min/max from separate attributes (e.g. read_cpu_min + read_cpu_max) in one SET.
    TResourceQuotaRange existing = target.get(type);
    long mergedMin = min;
    long mergedMax = max;
    if (existing != null) {
      if (min < 0) {
        mergedMin = existing.getMinValue();
      }
      if (max < 0) {
        mergedMax = existing.getMaxValue();
      }
    }
    target.put(type, new TResourceQuotaRange(mergedMin, mergedMax));
  }

  public void putDiskIo(OperationSide side, TTimedQuota timedQuota) {
    if (!userResourceQuota.isSetThrottleLimit()) {
      userResourceQuota.setThrottleLimit(new java.util.HashMap<>());
    }
    userResourceQuota
        .getThrottleLimit()
        .put(
            side == OperationSide.READ ? ThrottleType.READ_SIZE : ThrottleType.WRITE_SIZE,
            timedQuota);
  }

  public enum OperationSide {
    READ,
    WRITE
  }

  public enum ResourceSide {
    CPU,
    MEMORY,
    TEMP_DISK
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.OTHER;
  }

  @Override
  public List<org.apache.iotdb.commons.path.PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitSetUserResourceQuota(this, context);
  }
}
