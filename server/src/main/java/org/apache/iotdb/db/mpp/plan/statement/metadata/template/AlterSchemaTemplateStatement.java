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
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.List;

public class AlterSchemaTemplateStatement extends Statement implements IConfigStatement {

  private String templateName;
  private List<String> measurements;
  private List<TSDataType> dataTypes;
  private List<TSEncoding> encodings;
  private List<CompressionType> compressors;

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
    this.templateName = templateName;
    this.measurements = measurements;
    this.dataTypes = dataTypes;
    this.encodings = encodings;
    this.compressors = compressors;
    this.operationType = operationType;
  }

  public String getTemplateName() {
    return templateName;
  }

  public List<String> getMeasurements() {
    return measurements;
  }

  public List<TSEncoding> getEncodings() {
    return encodings;
  }

  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }

  public List<CompressionType> getCompressors() {
    return compressors;
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
    return null;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitAlterSchemaTemplate(this, context);
  }
}
