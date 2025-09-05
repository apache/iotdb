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

package org.apache.iotdb.db.queryengine.plan.statement.metadata.model;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import java.util.Collections;
import java.util.List;
import java.util.Objects;

public class UnloadModelStatement extends Statement implements IConfigStatement {

  private final String modelId;
  private final List<String> deviceIdList;

  public UnloadModelStatement(String modelId, List<String> deviceIdList) {
    this.modelId = modelId;
    this.deviceIdList = deviceIdList;
  }

  public String getModelId() {
    return modelId;
  }

  public List<String> getDeviceIdList() {
    return deviceIdList;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitUnloadModel(this, context);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    UnloadModelStatement that = (UnloadModelStatement) o;
    return Objects.equals(modelId, that.modelId) && Objects.equals(deviceIdList, that.deviceIdList);
  }

  @Override
  public int hashCode() {
    return Objects.hash(modelId, deviceIdList);
  }

  @Override
  public String toString() {
    return "UnloadModelStatement{"
        + "modelId='"
        + modelId
        + '\''
        + ", deviceIdList="
        + deviceIdList
        + '}';
  }
}
