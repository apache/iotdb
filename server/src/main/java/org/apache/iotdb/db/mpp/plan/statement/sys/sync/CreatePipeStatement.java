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
import org.apache.iotdb.commons.sync.SyncConstant;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.constant.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CreatePipeStatement extends Statement implements IConfigStatement {

  private String pipeName;
  private String pipeSinkName;
  private long startTime;
  private Map<String, String> pipeAttributes;

  public CreatePipeStatement(StatementType createPipeStatement) {
    this.statementType = createPipeStatement;
  }

  public String getPipeName() {
    return pipeName;
  }

  public String getPipeSinkName() {
    return pipeSinkName;
  }

  public long getStartTime() {
    return startTime;
  }

  public Map<String, String> getPipeAttributes() {
    return pipeAttributes;
  }

  public void setPipeName(String pipeName) {
    this.pipeName = pipeName;
  }

  public void setPipeSinkName(String pipeSinkName) {
    this.pipeSinkName = pipeSinkName;
  }

  public void setStartTime(long startTime) {
    this.startTime = startTime;
  }

  public void setPipeAttributes(Map<String, String> pipeAttributes) {
    this.pipeAttributes = pipeAttributes;
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
    return visitor.visitCreatePipe(this, context);
  }

  public static CreatePipeStatement parseString(String parsedString) throws IOException {
    String[] split = parsedString.split(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
    if (split.length < 4) {
      throw new IOException("Parsing CreatePipePlan error. Attributes is less than expected.");
    }
    CreatePipeStatement statement = new CreatePipeStatement(StatementType.CREATE_PIPE);
    statement.setPipeName(split[0]);
    statement.setPipeSinkName(split[1]);
    statement.setStartTime(Long.parseLong(split[2]));
    int size = (Integer.parseInt(split[3]) << 1);
    if (split.length != (size + 4)) {
      throw new IOException("Parsing CreatePipePlan error. Attributes number is wrong.");
    }
    Map<String, String> attributes = new HashMap<>();
    for (int i = 0; i < size; i += 2) {
      attributes.put(split[i + 4], split[i + 5]);
    }
    statement.setPipeAttributes(attributes);
    return statement;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(pipeName).append(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
    builder.append(pipeSinkName).append(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
    builder.append(startTime).append(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
    builder.append(pipeAttributes.size()).append(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
    for (Map.Entry<String, String> entry : pipeAttributes.entrySet()) {
      builder.append(entry.getKey()).append(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
      builder.append(entry.getValue()).append(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
    }
    return builder.toString();
  }
}
