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
import org.apache.iotdb.db.mpp.plan.analyze.QueryType;
import org.apache.iotdb.db.mpp.plan.statement.IConfigStatement;
import org.apache.iotdb.db.mpp.plan.statement.Statement;
import org.apache.iotdb.db.mpp.plan.statement.StatementType;
import org.apache.iotdb.db.mpp.plan.statement.StatementVisitor;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.List;
import java.util.stream.Collectors;

public class CreateSchemaTemplateStatement extends Statement implements IConfigStatement {

  String name;
  List<String> measurements;
  List<TSDataType> dataTypes;
  List<TSEncoding> encodings;
  List<CompressionType> compressors;
  boolean isAligned;

  // constant to help resolve serialized sequence
  private static final int NEW_PLAN = -1;

  public CreateSchemaTemplateStatement() {
    super();
    statementType = StatementType.CREATE_TEMPLATE;
  }

  public CreateSchemaTemplateStatement(
      String name,
      List<List<String>> measurements,
      List<List<TSDataType>> dataTypes,
      List<List<TSEncoding>> encodings,
      List<List<CompressionType>> compressors,
      boolean aligned) {
    // Only accessed by deserialization, which may cause ambiguity with align designation
    this();
    this.name = name;
    this.isAligned = aligned;
    if (aligned) {
      this.measurements = measurements.get(0);
      this.dataTypes = dataTypes.get(0);
      this.encodings = encodings.get(0);
      this.compressors = compressors.get(0);
    } else {
      this.measurements = measurements.stream().map(i -> i.get(0)).collect(Collectors.toList());
      this.dataTypes = dataTypes.stream().map(i -> i.get(0)).collect(Collectors.toList());
      this.encodings = encodings.stream().map(i -> i.get(0)).collect(Collectors.toList());
      this.compressors = compressors.stream().map(i -> i.get(0)).collect(Collectors.toList());
    }
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return null;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<String> getMeasurements() {
    return measurements;
  }

  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }

  public List<TSEncoding> getEncodings() {
    return encodings;
  }

  public List<CompressionType> getCompressors() {
    return compressors;
  }

  public boolean isAligned() {
    return isAligned;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCreateSchemaTemplate(this, context);
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }
}
