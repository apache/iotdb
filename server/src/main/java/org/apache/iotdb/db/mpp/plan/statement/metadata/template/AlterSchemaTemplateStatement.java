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

package org.apache.iotdb.db.mpp.plan.statement.metadata.template;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.template.TemplateAlterOperationType;
import org.apache.iotdb.db.metadata.template.alter.TemplateAlterInfo;
import org.apache.iotdb.db.metadata.template.alter.TemplateExtendInfo;
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.Collections;
import java.util.List;

public class AlterSchemaTemplateStatement extends Statement implements IConfigStatement {

  private TemplateAlterInfo templateAlterInfo;

  private TemplateAlterOperationType operationType;

  public AlterSchemaTemplateStatement() {
    super();
    statementType = StatementType.ALTER_TEMPLATE;
  }

  public AlterSchemaTemplateStatement(
      String templateName,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      TemplateAlterOperationType operationType) {
    this();
    if (operationType.equals(TemplateAlterOperationType.EXTEND_TEMPLATE)) {
      this.templateAlterInfo =
          new TemplateExtendInfo(templateName, measurements, dataTypes, encodings, compressors);
    }
    this.operationType = operationType;
  }

  public TemplateAlterInfo getTemplateAlterInfo() {
    return templateAlterInfo;
  }

  public TemplateAlterOperationType getOperationType() {
    return operationType;
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
    return visitor.visitAlterSchemaTemplate(this, context);
  }
}
