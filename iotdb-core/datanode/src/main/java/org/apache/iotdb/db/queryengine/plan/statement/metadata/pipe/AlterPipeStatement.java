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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Collections;
import java.util.List;
import java.util.Map;

public class AlterPipeStatement extends Statement implements IConfigStatement {

  private String pipeName;
  private Map<String, String> extractorAttributes;
  private Map<String, String> processorAttributes;
  private Map<String, String> connectorAttributes;
  private boolean isReplaceAllExtractorAttributes;
  private boolean isReplaceAllProcessorAttributes;
  private boolean isReplaceAllConnectorAttributes;

  public AlterPipeStatement(StatementType alterPipeStatement) {
    this.statementType = alterPipeStatement;
  }

  public String getPipeName() {
    return pipeName;
  }

  public Map<String, String> getExtractorAttributes() {
    return extractorAttributes;
  }

  public Map<String, String> getProcessorAttributes() {
    return processorAttributes;
  }

  public Map<String, String> getConnectorAttributes() {
    return connectorAttributes;
  }

  public boolean isReplaceAllExtractorAttributes() {
    return isReplaceAllExtractorAttributes;
  }

  public boolean isReplaceAllProcessorAttributes() {
    return isReplaceAllProcessorAttributes;
  }

  public boolean isReplaceAllConnectorAttributes() {
    return isReplaceAllConnectorAttributes;
  }

  public void setPipeName(String pipeName) {
    this.pipeName = pipeName;
  }

  public void setExtractorAttributes(Map<String, String> extractorAttributes) {
    this.extractorAttributes = extractorAttributes;
  }

  public void setProcessorAttributes(Map<String, String> processorAttributes) {
    this.processorAttributes = processorAttributes;
  }

  public void setConnectorAttributes(Map<String, String> connectorAttributes) {
    this.connectorAttributes = connectorAttributes;
  }

  public void setReplaceAllExtractorAttributes(boolean replaceAllExtractorAttributes) {
    isReplaceAllExtractorAttributes = replaceAllExtractorAttributes;
  }

  public void setReplaceAllProcessorAttributes(boolean replaceAllProcessorAttributes) {
    isReplaceAllProcessorAttributes = replaceAllProcessorAttributes;
  }

  public void setReplaceAllConnectorAttributes(boolean replaceAllConnectorAttributes) {
    isReplaceAllConnectorAttributes = replaceAllConnectorAttributes;
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
  public TSStatus checkPermissionBeforeProcess(String userName) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkSystemPermission(userName, PrivilegeType.USE_PIPE.ordinal()),
        PrivilegeType.USE_PIPE);
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitAlterPipe(this, context);
  }
}
