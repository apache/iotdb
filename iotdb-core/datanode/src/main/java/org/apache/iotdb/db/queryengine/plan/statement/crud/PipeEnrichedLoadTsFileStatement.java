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
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.io.File;
import java.util.List;

public class PipeEnrichedLoadTsFileStatement extends LoadTsFileStatement {

  private final LoadTsFileStatement loadTsFileStatement;

  public PipeEnrichedLoadTsFileStatement(LoadTsFileStatement loadTsFileStatement) {
    this.loadTsFileStatement = loadTsFileStatement;
    statementType = StatementType.MULTI_BATCH_INSERT;
  }

  public LoadTsFileStatement getLoadTsFileStatement() {
    return loadTsFileStatement;
  }

  @Override
  public boolean isDebug() {
    return loadTsFileStatement.isDebug();
  }

  @Override
  public void setDebug(boolean debug) {
    loadTsFileStatement.setDebug(debug);
  }

  @Override
  public boolean isQuery() {
    return loadTsFileStatement.isQuery();
  }

  @Override
  public boolean isAuthenticationRequired() {
    return loadTsFileStatement.isAuthenticationRequired();
  }

  @Override
  public void setDeleteAfterLoad(boolean deleteAfterLoad) {
    loadTsFileStatement.setDeleteAfterLoad(deleteAfterLoad);
  }

  @Override
  public void setDatabaseLevel(int databaseLevel) {
    loadTsFileStatement.setDatabaseLevel(databaseLevel);
  }

  @Override
  public void setVerifySchema(boolean verifySchema) {
    loadTsFileStatement.setVerifySchema(verifySchema);
  }

  @Override
  public void setAutoCreateDatabase(boolean autoCreateDatabase) {
    loadTsFileStatement.setAutoCreateDatabase(autoCreateDatabase);
  }

  @Override
  public boolean isVerifySchema() {
    return loadTsFileStatement.isVerifySchema();
  }

  @Override
  public boolean isDeleteAfterLoad() {
    return loadTsFileStatement.isDeleteAfterLoad();
  }

  @Override
  public boolean isAutoCreateDatabase() {
    return loadTsFileStatement.isAutoCreateDatabase();
  }

  @Override
  public int getDatabaseLevel() {
    return loadTsFileStatement.getDatabaseLevel();
  }

  @Override
  public List<File> getTsFiles() {
    return loadTsFileStatement.getTsFiles();
  }

  @Override
  public void addTsFileResource(TsFileResource resource) {
    loadTsFileStatement.addTsFileResource(resource);
  }

  @Override
  public List<TsFileResource> getResources() {
    return loadTsFileStatement.getResources();
  }

  @Override
  public List<PartialPath> getPaths() {
    return loadTsFileStatement.getPaths();
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitPipeEnrichedLoadFile(this, context);
  }

  @Override
  public String toString() {
    return "PipeEnrichedLoadTsFileStatement{" + "loadTsFileStatement=" + loadTsFileStatement + '}';
  }
}
