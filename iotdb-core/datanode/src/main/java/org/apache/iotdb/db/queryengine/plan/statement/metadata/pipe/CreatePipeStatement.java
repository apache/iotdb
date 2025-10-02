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

package org.apache.iotdb.db.queryengine.plan.statement.metadata.pipe;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class CreatePipeStatement extends Statement implements IConfigStatement {

  private String pipeName;
  private boolean ifNotExistsCondition;
  private Map<String, String> sourceAttributes;
  private Map<String, String> processorAttributes;
  private Map<String, String> sinkAttributes;

  public CreatePipeStatement(final StatementType createPipeStatement) {
    this.statementType = createPipeStatement;
  }

  public String getPipeName() {
    return pipeName;
  }

  public boolean hasIfNotExistsCondition() {
    return ifNotExistsCondition;
  }

  public Map<String, String> getSourceAttributes() {
    return sourceAttributes;
  }

  public Map<String, String> getProcessorAttributes() {
    return processorAttributes;
  }

  public Map<String, String> getSinkAttributes() {
    return sinkAttributes;
  }

  public void setPipeName(final String pipeName) {
    this.pipeName = pipeName;
  }

  public void setIfNotExists(boolean ifNotExistsCondition) {
    this.ifNotExistsCondition = ifNotExistsCondition;
  }

  public void setSourceAttributes(final Map<String, String> sourceAttributes) {
    this.sourceAttributes = sourceAttributes;
  }

  public void setProcessorAttributes(final Map<String, String> processorAttributes) {
    this.processorAttributes = processorAttributes;
  }

  public void setSinkAttributes(final Map<String, String> sinkAttributes) {
    this.sinkAttributes = sinkAttributes;
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
  public <R, C> R accept(final StatementVisitor<R, C> visitor, final C context) {
    return visitor.visitCreatePipe(this, context);
  }
}
