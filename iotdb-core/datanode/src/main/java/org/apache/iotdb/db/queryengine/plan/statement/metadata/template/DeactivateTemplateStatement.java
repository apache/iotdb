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

package org.apache.iotdb.db.queryengine.plan.statement.metadata.template;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.IoTDBException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.schemaengine.template.ClusterTemplateManager;
import org.apache.iotdb.db.schemaengine.template.Template;
import org.apache.iotdb.rpc.TSStatusCode;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

public class DeactivateTemplateStatement extends Statement implements IConfigStatement {

  private String templateName;

  private List<PartialPath> pathPatternList;

  public DeactivateTemplateStatement() {
    super();
    statementType = StatementType.DEACTIVATE_TEMPLATE;
  }

  public DeactivateTemplateStatement(String templateName, List<PartialPath> pathPatternList) {
    this.templateName = templateName;
    this.pathPatternList = pathPatternList;
    statementType = StatementType.DEACTIVATE_TEMPLATE;
  }

  @Override
  public List<PartialPath> getPaths() {
    ClusterTemplateManager clusterTemplateManager = ClusterTemplateManager.getInstance();

    Template template;

    try {
      template = clusterTemplateManager.getTemplate(templateName);
      if (template == null) {
        return Collections.emptyList();
      }
    } catch (IoTDBException e) {
      throw new RuntimeException(e);
    }

    return pathPatternList.stream()
        .flatMap(
            path -> template.getSchemaMap().keySet().stream().map(path::concatAsMeasurementPath))
        .collect(Collectors.toList());
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    List<PartialPath> checkedPaths = getPaths();
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkPatternPermission(
            userName, checkedPaths, PrivilegeType.WRITE_SCHEMA.ordinal()),
        checkedPaths,
        PrivilegeType.WRITE_SCHEMA);
  }

  public String getTemplateName() {
    return templateName == null ? ONE_LEVEL_PATH_WILDCARD : templateName;
  }

  public void setTemplateName(String templateName) {
    this.templateName = templateName;
  }

  public List<PartialPath> getPathPatternList() {
    return pathPatternList;
  }

  public void setPathPatternList(List<PartialPath> pathPatternList) {
    this.pathPatternList = pathPatternList;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitDeactivateTemplate(this, context);
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }
}
