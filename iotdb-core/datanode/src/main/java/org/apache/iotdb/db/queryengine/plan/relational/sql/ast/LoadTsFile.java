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

package org.apache.iotdb.db.queryengine.plan.relational.sql.ast;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static java.util.Objects.requireNonNull;

public class LoadTsFile extends Statement {
  private final String filePath;

  private final File file;
  private int databaseLevel; // For loading to tree-model only
  private String database; // For loading to table-model only
  private boolean deleteAfterLoad;
  private boolean autoCreateDatabase;
  private String model = LoadTsFileConfigurator.MODEL_TABLE_VALUE;

  private final Map<String, String> loadAttributes;

  private final List<File> tsFiles;
  private final List<TsFileResource> resources;
  private final List<Long> writePointCountList;

  public LoadTsFile(NodeLocation location, String filePath, Map<String, String> loadAttributes) {
    super(location);
    this.filePath = requireNonNull(filePath, "filePath is null");

    this.file = new File(filePath);
    this.databaseLevel = IoTDBDescriptor.getInstance().getConfig().getDefaultStorageGroupLevel();
    this.deleteAfterLoad = false;
    this.autoCreateDatabase = IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();
    this.resources = new ArrayList<>();
    this.writePointCountList = new ArrayList<>();
    this.loadAttributes = loadAttributes;
    initAttributes();

    try {
      this.tsFiles =
          org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement.processTsFile(
              file);
    } catch (FileNotFoundException e) {
      throw new SemanticException(e);
    }
  }

  public String getFilePath() {
    return filePath;
  }

  public Map<String, String> getLoadAttributes() {
    return loadAttributes;
  }

  public void setAutoCreateDatabase(boolean autoCreateDatabase) {
    this.autoCreateDatabase = autoCreateDatabase;
  }

  public boolean isDeleteAfterLoad() {
    return deleteAfterLoad;
  }

  public boolean isAutoCreateDatabase() {
    return autoCreateDatabase;
  }

  public int getDatabaseLevel() {
    return databaseLevel;
  }

  public String getDatabase() {
    return database;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getModel() {
    return model;
  }

  public List<File> getTsFiles() {
    return tsFiles;
  }

  public void addTsFileResource(TsFileResource resource) {
    resources.add(resource);
  }

  public List<TsFileResource> getResources() {
    return resources;
  }

  public void addWritePointCount(long writePointCount) {
    writePointCountList.add(writePointCount);
  }

  public long getWritePointCount(int resourceIndex) {
    return writePointCountList.get(resourceIndex);
  }

  private void initAttributes() {
    this.databaseLevel = LoadTsFileConfigurator.parseOrGetDefaultDatabaseLevel(loadAttributes);
    this.database = LoadTsFileConfigurator.parseDatabaseName(loadAttributes);
    this.deleteAfterLoad = LoadTsFileConfigurator.parseOrGetDefaultOnSuccess(loadAttributes);
    this.model =
        LoadTsFileConfigurator.parseOrGetDefaultModel(
            loadAttributes, LoadTsFileConfigurator.MODEL_TABLE_VALUE);
  }

  @Override
  public <R, C> R accept(AstVisitor<R, C> visitor, C context) {
    return visitor.visitLoadTsFile(this, context);
  }

  @Override
  public List<? extends Node> getChildren() {
    return Collections.emptyList();
  }

  @Override
  public int hashCode() {
    return Objects.hash(filePath, loadAttributes);
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) {
      return true;
    }
    if ((obj == null) || (getClass() != obj.getClass())) {
      return false;
    }
    LoadTsFile other = (LoadTsFile) obj;
    return Objects.equals(filePath, other.filePath)
        && Objects.equals(loadAttributes, other.loadAttributes);
  }

  @Override
  public String toString() {
    return toStringHelper(this)
        .add("filePath", filePath)
        .add("loadAttributes", loadAttributes)
        .toString();
  }
}
