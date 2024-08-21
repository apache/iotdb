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

package org.apache.iotdb.db.queryengine.plan.statement.metadata;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/** CREATE MULTI TIMESERIES statement. */
public class CreateMultiTimeSeriesStatement extends Statement {

  private List<MeasurementPath> paths;
  private List<TSDataType> dataTypes = new ArrayList<>();
  private List<TSEncoding> encodings = new ArrayList<>();
  private List<CompressionType> compressors = new ArrayList<>();
  private List<Map<String, String>> propsList;
  private List<String> aliasList;
  private List<Map<String, String>> tagsList;
  private List<Map<String, String>> attributesList;

  public CreateMultiTimeSeriesStatement() {
    super();
    statementType = StatementType.CREATE_MULTI_TIME_SERIES;
  }

  @Override
  public List<MeasurementPath> getPaths() {
    return paths;
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {
    if (AuthorityChecker.SUPER_USER.equals(userName)) {
      return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
    }
    List<MeasurementPath> checkedPaths = getPaths();
    return AuthorityChecker.getTSStatus(
        AuthorityChecker.checkFullPathListPermission(
            userName, checkedPaths, PrivilegeType.WRITE_SCHEMA.ordinal()),
        checkedPaths,
        PrivilegeType.WRITE_SCHEMA);
  }

  public void setPaths(List<MeasurementPath> paths) {
    this.paths = paths;
  }

  public List<TSDataType> getDataTypes() {
    return dataTypes;
  }

  public void setDataTypes(List<TSDataType> dataTypes) {
    this.dataTypes = dataTypes;
  }

  public List<TSEncoding> getEncodings() {
    return encodings;
  }

  public void setEncodings(List<TSEncoding> encodings) {
    this.encodings = encodings;
  }

  public List<CompressionType> getCompressors() {
    return compressors;
  }

  public void setCompressors(List<CompressionType> compressors) {
    this.compressors = compressors;
  }

  public List<Map<String, String>> getPropsList() {
    return propsList;
  }

  public void setPropsList(List<Map<String, String>> propsList) {
    this.propsList = propsList;
  }

  public List<String> getAliasList() {
    return aliasList;
  }

  public void setAliasList(List<String> aliasList) {
    this.aliasList = aliasList;
  }

  public List<Map<String, String>> getTagsList() {
    return tagsList;
  }

  public void setTagsList(List<Map<String, String>> tagsList) {
    this.tagsList = tagsList;
  }

  public List<Map<String, String>> getAttributesList() {
    return attributesList;
  }

  public void setAttributesList(List<Map<String, String>> attributesList) {
    this.attributesList = attributesList;
  }

  public void addAttributesList(Map<String, String> attributes) {
    this.attributesList.add(attributes);
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitCreateMultiTimeSeries(this, context);
  }

  @Override
  public String toString() {
    return "CreateMultiTimeSeriesStatement{"
        + "paths="
        + paths
        + ", dataTypes="
        + dataTypes
        + ", encodings="
        + encodings
        + ", compressors="
        + compressors
        + ", propsList="
        + propsList
        + ", aliasList="
        + aliasList
        + ", tagsList="
        + tagsList
        + ", attributesList="
        + attributesList
        + "}";
  }
}
