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

package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.qp.logical.Operator;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTemplatePlan;
import org.apache.iotdb.db.qp.strategy.PhysicalGenerator;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.ArrayList;
import java.util.List;

public class CreateTemplateOperator extends Operator {

  String name;
  List<String> schemaNames = new ArrayList<>();
  List<List<String>> measurements = new ArrayList<>();
  List<List<TSDataType>> dataTypes = new ArrayList<>();
  List<List<TSEncoding>> encodings = new ArrayList<>();
  List<List<CompressionType>> compressors = new ArrayList<>();

  public CreateTemplateOperator(int tokenIntType) {
    super(tokenIntType);
    operatorType = OperatorType.CREATE_TEMPLATE;
  }

  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public List<String> getSchemaNames() {
    return schemaNames;
  }

  public void setSchemaNames(List<String> schemaNames) {
    this.schemaNames = schemaNames;
  }

  public void addSchemaName(String schemaName) {
    this.schemaNames.add(schemaName);
  }

  public List<List<String>> getMeasurements() {
    return measurements;
  }

  public void setMeasurements(List<List<String>> measurements) {
    this.measurements = measurements;
  }

  public void addMeasurements(List<String> measurements) {
    this.measurements.add(measurements);
  }

  public List<List<TSDataType>> getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(List<List<TSDataType>> dataTypes) {
    this.dataTypes = dataTypes;
  }

  public void addDataTypes(List<TSDataType> dataTypes) {
    this.dataTypes.add(dataTypes);
  }

  public List<List<TSEncoding>> getEncodings() {
    return encodings;
  }

  public void setEncodings(List<List<TSEncoding>> encodings) {
    this.encodings = encodings;
  }

  public void addEncodings(List<TSEncoding> encodings) {
    this.encodings.add(encodings);
  }

  public List<List<CompressionType>> getCompressors() {
    return compressors;
  }

  public void setCompressors(List<List<CompressionType>> compressors) {
    this.compressors = compressors;
  }

  public void addCompressor(List<CompressionType> compressors) {
    this.compressors.add(compressors);
  }

  @Override
  public PhysicalPlan generatePhysicalPlan(PhysicalGenerator generator)
      throws QueryProcessException {
    return new CreateTemplatePlan(
        name, schemaNames, measurements, dataTypes, encodings, compressors);
  }
}
