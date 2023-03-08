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

public class CreatePipeStatement extends Statement implements IConfigStatement {

  private String pipeName;
  private Map<String, String> collectorAttributes;
  private Map<String, String> processorAttributes;
  private Map<String, String> connectorAttributes;

  public CreatePipeStatement(StatementType createPipeStatement) {
    this.statementType = createPipeStatement;
  }

  public String getPipeName() {
    return pipeName;
  }

  public Map<String, String> getCollectorAttributes() {
    return collectorAttributes;
  }

  public Map<String, String> getProcessorAttributes() {
    return processorAttributes;
  }

  public Map<String, String> getConnectorAttributes() {
    return connectorAttributes;
  }

  public void setPipeName(String pipeName) {
    this.pipeName = pipeName;
  }

  public void setCollectorAttributes(Map<String, String> collectorAttributes) {
    this.collectorAttributes = collectorAttributes;
  }

  public void setProcessorAttributes(Map<String, String> processorAttributes) {
    this.processorAttributes = processorAttributes;
  }

  public void setConnectorAttributes(Map<String, String> connectorAttributes) {
    this.connectorAttributes = connectorAttributes;
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
    int collectorSize = (Integer.parseInt(split[1]) << 1);
    int processorSize = (Integer.parseInt(split[2]) << 1);
    int connectorSize = (Integer.parseInt(split[3]) << 1);
    if (split.length != (collectorSize + processorSize + connectorSize + 4)) {
      throw new IOException("Parsing CreatePipePlan error. Attributes number is wrong.");
    }
    Map<String, String> collectorAttributes = new HashMap<>();
    for (int i = 4; i < collectorSize + 4; i += 2) {
      collectorAttributes.put(split[i], split[i + 1]);
    }
    statement.setCollectorAttributes(collectorAttributes);
    Map<String, String> processorAttributes = new HashMap<>();
    for (int i = collectorSize + 4; i < collectorSize + processorSize + 4; i += 2) {
      processorAttributes.put(split[i], split[i + 1]);
    }
    statement.setCollectorAttributes(processorAttributes);
    Map<String, String> connectorAttributes = new HashMap<>();
    for (int i = collectorSize + processorSize + 4;
        i < collectorSize + processorSize + connectorSize + 4;
        i += 2) {
      connectorAttributes.put(split[i], split[i + 1]);
    }
    statement.setCollectorAttributes(connectorAttributes);
    return statement;
  }

  @Override
  public String toString() {
    StringBuilder builder = new StringBuilder();
    builder.append(pipeName).append(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
    builder.append(collectorAttributes.size()).append(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
    builder.append(processorAttributes.size()).append(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
    builder.append(connectorAttributes.size()).append(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
    for (Map.Entry<String, String> entry : collectorAttributes.entrySet()) {
      builder.append(entry.getKey()).append(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
      builder.append(entry.getValue()).append(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
    }
    for (Map.Entry<String, String> entry : processorAttributes.entrySet()) {
      builder.append(entry.getKey()).append(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
      builder.append(entry.getValue()).append(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
    }
    for (Map.Entry<String, String> entry : connectorAttributes.entrySet()) {
      builder.append(entry.getKey()).append(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
      builder.append(entry.getValue()).append(SyncConstant.PLAN_SERIALIZE_SPLIT_CHARACTER);
    }
    return builder.toString();
  }
}
