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

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator;

import org.apache.tsfile.annotations.TableModel;
import org.apache.tsfile.common.constant.TsFileConstant;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator.ASYNC_LOAD_KEY;
import static org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator.CONVERT_ON_TYPE_MISMATCH_KEY;
import static org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator.DATABASE_LEVEL_KEY;
import static org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator.DATABASE_NAME_KEY;
import static org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator.ON_SUCCESS_DELETE_VALUE;
import static org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator.ON_SUCCESS_KEY;
import static org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator.ON_SUCCESS_NONE_VALUE;
import static org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator.PIPE_GENERATED_KEY;
import static org.apache.iotdb.db.storageengine.load.config.LoadTsFileConfigurator.TABLET_CONVERSION_THRESHOLD_KEY;

public class LoadTsFileStatement extends Statement {

  private final File file;
  private int databaseLevel; // For loading to tree-model only
  private String database; // For loading to table-model only
  private boolean verifySchema = true;
  private boolean deleteAfterLoad = false;
  private boolean convertOnTypeMismatch = true;
  private long tabletConversionThresholdBytes = -1;
  private boolean autoCreateDatabase = true;
  private boolean isGeneratedByPipe = false;
  private boolean isAsyncLoad = false;

  private List<File> tsFiles;
  private List<Boolean> isTableModel;
  private List<TsFileResource> resources;
  private List<Long> writePointCountList;

  public LoadTsFileStatement(String filePath) throws FileNotFoundException {
    this.file = new File(filePath).getAbsoluteFile();
    this.databaseLevel = IoTDBDescriptor.getInstance().getConfig().getDefaultDatabaseLevel();
    this.verifySchema = true;
    this.deleteAfterLoad = false;
    this.convertOnTypeMismatch = true;
    this.tabletConversionThresholdBytes =
        IoTDBDescriptor.getInstance().getConfig().getLoadTabletConversionThresholdBytes();
    this.autoCreateDatabase = IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();

    this.tsFiles = processTsFile(file);
    this.resources = new ArrayList<>();
    this.writePointCountList = new ArrayList<>();
    this.isTableModel = new ArrayList<>(Collections.nCopies(this.tsFiles.size(), false));
    this.statementType = StatementType.MULTI_BATCH_INSERT;
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
    this.databaseLevel = IoTDBDescriptor.getInstance().getConfig().getDefaultDatabaseLevel();
    this.verifySchema = true;
    this.deleteAfterLoad = false;
    this.convertOnTypeMismatch = true;
    this.tabletConversionThresholdBytes =
        IoTDBDescriptor.getInstance().getConfig().getLoadTabletConversionThresholdBytes();
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

  public void setDatabaseLevel(int databaseLevel) {
    this.databaseLevel = databaseLevel;
  }

  public int getDatabaseLevel() {
    return databaseLevel;
  }

  public void setDatabase(String database) {
    this.database = database;
  }

  public String getDatabase() {
    return database;
  }

  public void setVerifySchema(boolean verifySchema) {
    this.verifySchema = verifySchema;
  }

  public boolean isVerifySchema() {
    return verifySchema;
  }

  public LoadTsFileStatement setDeleteAfterLoad(boolean deleteAfterLoad) {
    this.deleteAfterLoad = deleteAfterLoad;
    return this;
  }

  public boolean isDeleteAfterLoad() {
    return deleteAfterLoad;
  }

  public LoadTsFileStatement setConvertOnTypeMismatch(boolean convertOnTypeMismatch) {
    this.convertOnTypeMismatch = convertOnTypeMismatch;
    return this;
  }

  public boolean isConvertOnTypeMismatch() {
    return convertOnTypeMismatch;
  }

  public void setTabletConversionThresholdBytes(long tabletConversionThresholdBytes) {
    this.tabletConversionThresholdBytes = tabletConversionThresholdBytes;
  }

  public long getTabletConversionThresholdBytes() {
    return tabletConversionThresholdBytes;
  }

  public void setAutoCreateDatabase(boolean autoCreateDatabase) {
    this.autoCreateDatabase = autoCreateDatabase;
  }

  public boolean isAutoCreateDatabase() {
    return autoCreateDatabase;
  }

  public List<Boolean> getIsTableModel() {
    return isTableModel;
  }

  public void setIsTableModel(List<Boolean> isTableModel) {
    this.isTableModel = isTableModel;
  }

  public void markIsGeneratedByPipe() {
    isGeneratedByPipe = true;
  }

  public boolean isGeneratedByPipe() {
    return isGeneratedByPipe;
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
    initAttributes(loadAttributes);
  }

  public boolean isAsyncLoad() {
    return isAsyncLoad;
  }

  private void initAttributes(final Map<String, String> loadAttributes) {
    this.databaseLevel = LoadTsFileConfigurator.parseOrGetDefaultDatabaseLevel(loadAttributes);
    this.database = LoadTsFileConfigurator.parseDatabaseName(loadAttributes);
    this.deleteAfterLoad = LoadTsFileConfigurator.parseOrGetDefaultOnSuccess(loadAttributes);
    this.convertOnTypeMismatch =
        LoadTsFileConfigurator.parseOrGetDefaultConvertOnTypeMismatch(loadAttributes);
    this.tabletConversionThresholdBytes =
        LoadTsFileConfigurator.parseOrGetDefaultTabletConversionThresholdBytes(loadAttributes);
    this.verifySchema = LoadTsFileConfigurator.parseOrGetDefaultVerify(loadAttributes);
    this.isAsyncLoad = LoadTsFileConfigurator.parseOrGetDefaultAsyncLoad(loadAttributes);
    if (LoadTsFileConfigurator.parseOrGetDefaultPipeGenerated(loadAttributes)) {
      markIsGeneratedByPipe();
    }
  }

  public boolean reconstructStatementIfMiniFileConverted(final List<Boolean> isMiniTsFile) {
    int lastNonMiniTsFileIndex = -1;

    for (int i = 0, n = isMiniTsFile.size(); i < n; i++) {
      if (isMiniTsFile.get(i)) {
        continue;
      }
      ++lastNonMiniTsFileIndex;
      if (tsFiles != null) {
        tsFiles.set(lastNonMiniTsFileIndex, tsFiles.get(i));
      }
      if (isTableModel != null) {
        isTableModel.set(lastNonMiniTsFileIndex, isTableModel.get(i));
      }
      if (resources != null) {
        resources.set(lastNonMiniTsFileIndex, resources.get(i));
      }
      if (writePointCountList != null) {
        writePointCountList.set(lastNonMiniTsFileIndex, writePointCountList.get(i));
      }
    }

    tsFiles =
        tsFiles != null ? tsFiles.subList(0, lastNonMiniTsFileIndex + 1) : Collections.emptyList();
    isTableModel =
        isTableModel != null
            ? isTableModel.subList(0, lastNonMiniTsFileIndex + 1)
            : Collections.emptyList();
    resources =
        resources != null
            ? resources.subList(0, lastNonMiniTsFileIndex + 1)
            : Collections.emptyList();
    writePointCountList =
        writePointCountList != null
            ? writePointCountList.subList(0, lastNonMiniTsFileIndex + 1)
            : Collections.emptyList();

    return tsFiles == null || tsFiles.isEmpty();
  }

  @Override
  public boolean shouldSplit() {
    final int splitThreshold =
        IoTDBDescriptor.getInstance().getConfig().getLoadTsFileStatementSplitThreshold();
    return tsFiles.size() > splitThreshold && !isAsyncLoad;
  }

  /**
   * Splits the current LoadTsFileStatement into multiple sub-statements, each handling a batch of
   * TsFiles. Used to limit resource consumption during statement analysis, etc.
   *
   * @return the list of sub-statements
   */
  @Override
  public List<LoadTsFileStatement> getSubStatements() {
    final int batchSize =
        IoTDBDescriptor.getInstance().getConfig().getLoadTsFileSubStatementBatchSize();
    final int totalBatches = (tsFiles.size() + batchSize - 1) / batchSize; // Ceiling division
    final List<LoadTsFileStatement> subStatements = new ArrayList<>(totalBatches);

    for (int i = 0; i < tsFiles.size(); i += batchSize) {
      final int endIndex = Math.min(i + batchSize, tsFiles.size());
      final List<File> batchFiles = tsFiles.subList(i, endIndex);

      final LoadTsFileStatement statement = new LoadTsFileStatement();
      statement.databaseLevel = this.databaseLevel;
      statement.verifySchema = this.verifySchema;
      statement.deleteAfterLoad = this.deleteAfterLoad;
      statement.convertOnTypeMismatch = this.convertOnTypeMismatch;
      statement.tabletConversionThresholdBytes = this.tabletConversionThresholdBytes;
      statement.autoCreateDatabase = this.autoCreateDatabase;
      statement.isAsyncLoad = this.isAsyncLoad;
      statement.isGeneratedByPipe = this.isGeneratedByPipe;

      statement.tsFiles = new ArrayList<>(batchFiles);
      statement.resources = new ArrayList<>(batchFiles.size());
      statement.writePointCountList = new ArrayList<>(batchFiles.size());
      statement.isTableModel = new ArrayList<>(batchFiles.size());
      for (int j = 0; j < batchFiles.size(); j++) {
        statement.isTableModel.add(false);
      }

      subStatements.add(statement);
    }

    return subStatements;
  }

  @Override
  public List<PartialPath> getPaths() {
    return Collections.emptyList();
  }

  @TableModel
  @Override
  public org.apache.iotdb.db.queryengine.plan.relational.sql.ast.Statement toRelationalStatement(
      MPPQueryContext context) {
    final Map<String, String> loadAttributes = new HashMap<>();

    loadAttributes.put(DATABASE_LEVEL_KEY, String.valueOf(databaseLevel));
    if (database != null) {
      loadAttributes.put(DATABASE_NAME_KEY, database);
    }
    loadAttributes.put(
        ON_SUCCESS_KEY, deleteAfterLoad ? ON_SUCCESS_DELETE_VALUE : ON_SUCCESS_NONE_VALUE);
    loadAttributes.put(CONVERT_ON_TYPE_MISMATCH_KEY, String.valueOf(convertOnTypeMismatch));
    loadAttributes.put(
        TABLET_CONVERSION_THRESHOLD_KEY, String.valueOf(tabletConversionThresholdBytes));
    loadAttributes.put(ASYNC_LOAD_KEY, String.valueOf(isAsyncLoad));
    if (isGeneratedByPipe) {
      loadAttributes.put(PIPE_GENERATED_KEY, String.valueOf(true));
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
        + ", delete-after-load="
        + deleteAfterLoad
        + ", database-level="
        + databaseLevel
        + ", verify-schema="
        + verifySchema
        + ", convert-on-type-mismatch="
        + convertOnTypeMismatch
        + ", tablet-conversion-threshold="
        + tabletConversionThresholdBytes
        + ", async-load="
        + isAsyncLoad
        + ", tsFiles size="
        + tsFiles.size()
        + '}';
  }
}
