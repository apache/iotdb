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

package org.apache.iotdb.db.queryengine.plan.statement.crud;

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator;
import org.apache.iotdb.db.utils.annotations.TableModel;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.tsfile.common.constant.TsFileConstant;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator.DATABASE_LEVEL_KEY;
import static org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator.DATABASE_NAME_KEY;
import static org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator.MODEL_KEY;
import static org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator.ON_SUCCESS_KEY;

public class LoadTsFileStatement extends Statement {

  private final File file;
  private int databaseLevel; // For loading to tree-model only
  private String database; // For loading to table-model only
  private boolean verifySchema;
  private boolean deleteAfterLoad;
  private boolean autoCreateDatabase;
  private String model = LoadTsFileConfigurator.MODEL_TREE_VALUE;

  private Map<String, String> loadAttributes;

  private final List<File> tsFiles;
  private final List<TsFileResource> resources;
  private final List<Long> writePointCountList;

  public LoadTsFileStatement(String filePath) throws FileNotFoundException {
    this.file = new File(filePath);
    this.databaseLevel = IoTDBDescriptor.getInstance().getConfig().getDefaultStorageGroupLevel();
    this.verifySchema = true;
    this.deleteAfterLoad = false;
    this.autoCreateDatabase = IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();
    this.resources = new ArrayList<>();
    this.writePointCountList = new ArrayList<>();
    this.statementType = StatementType.MULTI_BATCH_INSERT;

    this.tsFiles = processTsFile(file);
  }

  public static List<File> processTsFile(final File file) throws FileNotFoundException {
    final List<File> tsFiles = new ArrayList<>();
    if (file.isFile()) {
      tsFiles.add(file);
    } else {
      if (file.listFiles() == null) {
        throw new FileNotFoundException(
            String.format(
                "Can not find %s on this machine, notice that load can only handle files on this machine.",
                file.getPath()));
      }
      tsFiles.addAll(findAllTsFile(file));
    }
    sortTsFiles(tsFiles);
    return tsFiles;
  }

  protected LoadTsFileStatement() {
    this.file = null;
    this.databaseLevel = IoTDBDescriptor.getInstance().getConfig().getDefaultStorageGroupLevel();
    this.verifySchema = true;
    this.deleteAfterLoad = false;
    this.autoCreateDatabase = IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();
    this.tsFiles = new ArrayList<>();
    this.resources = new ArrayList<>();
    this.writePointCountList = new ArrayList<>();
    this.statementType = StatementType.MULTI_BATCH_INSERT;
  }

  private static List<File> findAllTsFile(File file) {
    final File[] files = file.listFiles();
    if (files == null) {
      return Collections.emptyList();
    }

    final List<File> tsFiles = new ArrayList<>();
    for (File nowFile : files) {
      if (nowFile.getName().endsWith(TsFileConstant.TSFILE_SUFFIX)) {
        tsFiles.add(nowFile);
      } else if (nowFile.isDirectory()) {
        tsFiles.addAll(findAllTsFile(nowFile));
      }
    }
    return tsFiles;
  }

  private static void sortTsFiles(List<File> files) {
    files.sort(
        (o1, o2) -> {
          String file1Name = o1.getName();
          String file2Name = o2.getName();
          try {
            return TsFileResource.checkAndCompareFileName(file1Name, file2Name);
          } catch (IOException e) {
            return file1Name.compareTo(file2Name);
          }
        });
  }

  public void setDeleteAfterLoad(boolean deleteAfterLoad) {
    this.deleteAfterLoad = deleteAfterLoad;
  }

  public void setDatabaseLevel(int databaseLevel) {
    this.databaseLevel = databaseLevel;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public void setModel(String model) {
    this.model = model;
  }

  public void setVerifySchema(boolean verifySchema) {
    this.verifySchema = verifySchema;
  }

  public void setAutoCreateDatabase(boolean autoCreateDatabase) {
    this.autoCreateDatabase = autoCreateDatabase;
  }

  public boolean isVerifySchema() {
    return verifySchema;
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

  public void setLoadAttributes(final Map<String, String> loadAttributes) {
    this.loadAttributes = loadAttributes;
    initAttributes();
  }

  private void initAttributes() {
    this.databaseLevel = LoadTsFileConfigurator.parseOrGetDefaultDatabaseLevel(loadAttributes);
    this.database = LoadTsFileConfigurator.parseDatabaseName(loadAttributes);
    this.deleteAfterLoad = LoadTsFileConfigurator.parseOrGetDefaultOnSuccess(loadAttributes);
    this.model =
        LoadTsFileConfigurator.parseOrGetDefaultModel(
            loadAttributes, LoadTsFileConfigurator.MODEL_TREE_VALUE);
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @Override
  public TSStatus checkPermissionBeforeProcess(String userName) {
    // no need to check here, it will be checked in process phase
    return new TSStatus(TSStatusCode.SUCCESS_STATUS.getStatusCode());
  }

  @TableModel
  @Override
  public org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement toRelationalStatement(
      MPPQueryContext context) {
    loadAttributes = new HashMap<>();
    loadAttributes.put(DATABASE_LEVEL_KEY, String.valueOf(databaseLevel));
    if (database != null) {
      loadAttributes.put(DATABASE_NAME_KEY, database);
    }
    loadAttributes.put(ON_SUCCESS_KEY, String.valueOf(deleteAfterLoad));
    if (model != null) {
      loadAttributes.put(MODEL_KEY, model);
    }
    return new LoadTsFile(null, file.getAbsolutePath(), loadAttributes);
  }

  @Override
  public <R, C> R accept(StatementVisitor<R, C> visitor, C context) {
    return visitor.visitLoadFile(this, context);
  }

  @Override
  public String toString() {
    return "LoadTsFileStatement{"
        + "file="
        + file
        + ", deleteAfterLoad="
        + deleteAfterLoad
        + ", databaseLevel="
        + databaseLevel
        + ", verifySchema="
        + verifySchema
        + ", tsFiles Size="
        + tsFiles.size()
        + '}';
  }
}
