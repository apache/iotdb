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

package org.apache.iotdb.db.mpp.plan.statement.sys.sync;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.sync.utils.SyncConstant;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreatePipeSinkStatement extends Statement implements IConfigStatement {

  private String pipeSinkName;

  private String pipeSinkType;

  private Map<String, String> attributes;

  public CreatePipeSinkStatement() {
    this.statementType = StatementType.CREATE_PIPESINK;
  }

  public String getPipeSinkName() {
    return pipeSinkName;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public String getPipeSinkType() {
    return pipeSinkType;
  }

  public void setPipeSinkName(String pipeSinkName) {
    this.pipeSinkName = pipeSinkName;
  }

  public void setPipeSinkType(String pipeSinkType) {
    this.pipeSinkType = pipeSinkType;
  }

  public void setAttributes(Map<String, String> attributes) {
    this.attributes = attributes;
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCreatePipeSink(this, context);
  }

  public static CreatePipeSinkStatement parseString(String parsedString) throws IOException {
    String[] split = parsedString.split(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
    if (split.length < 3) {
      throw new IOException(
          "Parsing CreatePipeSinkStatement error. Attributes is less than expected.");
    }
    CreatePipeSinkStatement createPipeSinkStatement = new CreatePipeSinkStatement();
    createPipeSinkStatement.setPipeSinkName(split[0]);
    createPipeSinkStatement.setPipeSinkType(split[1]);
    int size = (Integer.parseInt(split[2]) << 1);
    if (split.length != (size + 3)) {
      throw new IOException("Parsing CreatePipeSinkPlan error. Attributes number is wrong.");
    }
    Map<String, String> attributes = new HashMap<>();
    for (int i = 0; i < size; i += 2) {
      attributes.put(split[i + 3], split[i + 4]);
    }
    createPipeSinkStatement.setAttributes(attributes);
    return createPipeSinkStatement;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(pipeSinkName).append(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
    builder.append(pipeSinkType).append(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
    builder.append(attributes.size()).append(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
    for (Map.Entry<String, String> entry : attributes.entrySet()) {
      builder.append(entry.getKey()).append(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
      builder.append(entry.getValue()).append(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
    }
    return builder.toString();
  }
}
