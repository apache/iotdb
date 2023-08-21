/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.queryengine.plan.statement.crud;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.exception.metadata.DataTypeMismatchException;
import org.apache.iotdb.db.exception.metadata.PathNotExistException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaValidation;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

import java.util.List;
import java.util.Map;

public class PipeEnrichedInsertBaseStatement extends InsertBaseStatement {

  private InsertBaseStatement insertBaseStatement;

  public PipeEnrichedInsertBaseStatement(InsertBaseStatement insertBaseStatement) {
    statementType = StatementType.PIPE_ENRICHED_INSERT;
    this.insertBaseStatement = insertBaseStatement;
  }

  public InsertBaseStatement getInsertBaseStatement() {
    return insertBaseStatement;
  }

  public void setInsertBaseStatement(InsertBaseStatement insertBaseStatement) {
    this.insertBaseStatement = insertBaseStatement;
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitPipeEnrichedInsert(this, context);
  }

  @Override
  public boolean isDebug() {
    return insertBaseStatement.isDebug();
  }

  @Override
  public void setDebug(boolean debug) {
    insertBaseStatement.setDebug(debug);
  }

  @Override
  public boolean isQuery() {
    return insertBaseStatement.isQuery();
  }

  @Override
  public boolean isAuthenticationRequired() {
    return insertBaseStatement.isAuthenticationRequired();
  }

  @Override
  public PartialPath getDevicePath() {
    return insertBaseStatement.getDevicePath();
  }

  @Override
  public void setDevicePath(PartialPath devicePath) {
    insertBaseStatement.setDevicePath(devicePath);
  }

  @Override
  public String[] getMeasurements() {
    return insertBaseStatement.getMeasurements();
  }

  @Override
  public void setMeasurements(String[] measurements) {
    insertBaseStatement.setMeasurements(measurements);
  }

  @Override
  public MeasurementSchema[] getMeasurementSchemas() {
    return insertBaseStatement.getMeasurementSchemas();
  }

  @Override
  public void setMeasurementSchemas(MeasurementSchema[] measurementSchemas) {
    insertBaseStatement.setMeasurementSchemas(measurementSchemas);
  }

  @Override
  public boolean isAligned() {
    return insertBaseStatement.isAligned();
  }

  @Override
  public void setAligned(boolean aligned) {
    insertBaseStatement.setAligned(aligned);
  }

  @Override
  public TSDataType[] getDataTypes() {
    return insertBaseStatement.getDataTypes();
  }

  @Override
  public void setDataTypes(TSDataType[] dataTypes) {
    insertBaseStatement.setDataTypes(dataTypes);
  }

  @Override
  public List<PartialPath> getPaths() {
    return insertBaseStatement.getPaths();
  }

  @Override
  public void updateAfterSchemaValidation() throws QueryProcessException {
    insertBaseStatement.updateAfterSchemaValidation();
  }

  @Override
  protected void selfCheckDataTypes(int index)
      throws DataTypeMismatchException, PathNotExistException {
    insertBaseStatement.selfCheckDataTypes(index);
  }

  @Override
  public void markFailedMeasurement(int index, Exception cause) {
    insertBaseStatement.markFailedMeasurement(index, cause);
  }

  @Override
  public boolean hasValidMeasurements() {
    return insertBaseStatement.hasValidMeasurements();
  }

  @Override
  public boolean hasFailedMeasurements() {
    return insertBaseStatement.hasFailedMeasurements();
  }

  @Override
  public int getFailedMeasurementNumber() {
    return insertBaseStatement.getFailedMeasurementNumber();
  }

  @Override
  public List<String> getFailedMeasurements() {
    return insertBaseStatement.getFailedMeasurements();
  }

  @Override
  public List<Exception> getFailedExceptions() {
    return insertBaseStatement.getFailedExceptions();
  }

  @Override
  public List<String> getFailedMessages() {
    return insertBaseStatement.getFailedMessages();
  }

  @Override
  public void setFailedMeasurementIndex2Info(
      Map<Integer, InsertBaseStatement.FailedMeasurementInfo> failedMeasurementIndex2Info) {
    insertBaseStatement.setFailedMeasurementIndex2Info(failedMeasurementIndex2Info);
  }

  @Override
  protected Map<PartialPath, List<Pair<String, Integer>>> getMapFromDeviceToMeasurementAndIndex() {
    return insertBaseStatement.getMapFromDeviceToMeasurementAndIndex();
  }

  @Override
  public boolean isEmpty() {
    return insertBaseStatement.isEmpty();
  }

  @Override
  public ISchemaValidation getSchemaValidation() {
    return insertBaseStatement.getSchemaValidation();
  }

  @Override
  public List<ISchemaValidation> getSchemaValidationList() {
    return insertBaseStatement.getSchemaValidationList();
  }

  @Override
  protected boolean checkAndCastDataType(int columnIndex, TSDataType dataType) {
    return insertBaseStatement.checkAndCastDataType(columnIndex, dataType);
  }

  @Override
  public long getMinTime() {
    return insertBaseStatement.getMinTime();
  }

  @Override
  public Object getFirstValueOfIndex(int index) {
    return insertBaseStatement.getFirstValueOfIndex(index);
  }

  @Override
  public InsertBaseStatement removeLogicalView() {
    return new PipeEnrichedInsertBaseStatement(insertBaseStatement.removeLogicalView());
  }
}
