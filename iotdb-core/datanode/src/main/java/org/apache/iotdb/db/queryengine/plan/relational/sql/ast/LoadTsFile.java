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

import org.apache.tsfile.utils.RamUsageEstimator;

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

  private static final long INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(LoadTsFile.class);

  private static final long FILE_INSTANCE_SIZE =
      RamUsageEstimator.shallowSizeOfInstance(File.class);

  private String filePath;

  private int databaseLevel; // For loading to tree-model only
  private String database; // For loading to table-model only
  private boolean deleteAfterLoad;
  private boolean convertOnTypeMismatch;
  private long tabletConversionThresholdBytes;
  private boolean autoCreateDatabase;
  private boolean verify;
  private boolean isAsyncLoad = false;

  private boolean isGeneratedByPipe = false;

  private Map<String, String> loadAttributes;

  private List<File> tsFiles;
  private List<TsFileResource> resources;
  private List<Long> writePointCountList;
  private List<Boolean> isTableModel;

  public LoadTsFile(NodeLocation location, String filePath, Map<String, String> loadAttributes) {
    super(location);
    this.filePath = requireNonNull(filePath, "filePath is null");

    this.databaseLevel = IoTDBDescriptor.getInstance().getConfig().getDefaultDatabaseLevel();
    this.deleteAfterLoad = false;
    this.convertOnTypeMismatch = true;
    this.tabletConversionThresholdBytes =
        IoTDBDescriptor.getInstance().getConfig().getLoadTabletConversionThresholdBytes();
    this.autoCreateDatabase = IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled();
    this.verify = true;

    this.loadAttributes = loadAttributes == null ? Collections.emptyMap() : loadAttributes;
    initAttributes();

    try {
      this.tsFiles =
          org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement.processTsFile(
              new File(filePath));
      this.resources = new ArrayList<>();
      this.writePointCountList = new ArrayList<>();
      this.isTableModel = new ArrayList<>(Collections.nCopies(this.tsFiles.size(), true));
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

  public boolean isAutoCreateDatabase() {
    return autoCreateDatabase;
  }

  public boolean isDeleteAfterLoad() {
    return deleteAfterLoad;
  }

  public LoadTsFile setDeleteAfterLoad(boolean deleteAfterLoad) {
    this.deleteAfterLoad = deleteAfterLoad;
    return this;
  }

  public boolean isConvertOnTypeMismatch() {
    return convertOnTypeMismatch;
  }

  public LoadTsFile setConvertOnTypeMismatch(boolean convertOnTypeMismatch) {
    this.convertOnTypeMismatch = convertOnTypeMismatch;
    return this;
  }

  public long getTabletConversionThresholdBytes() {
    return tabletConversionThresholdBytes;
  }

  public boolean isVerifySchema() {
    return verify;
  }

  public int getDatabaseLevel() {
    return databaseLevel;
  }

  public String getDatabase() {
    return database;
  }

  public LoadTsFile setDatabase(String database) {
    this.database = database;
    return this;
  }

  public boolean isAsyncLoad() {
    return isAsyncLoad;
  }

  public void markIsGeneratedByPipe() {
    isGeneratedByPipe = true;
  }

  public boolean isGeneratedByPipe() {
    return isGeneratedByPipe;
  }

  public List<Boolean> getIsTableModel() {
    return isTableModel;
  }

  public void setIsTableModel(List<Boolean> isTableModel) {
    this.isTableModel = isTableModel;
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
    this.convertOnTypeMismatch =
        LoadTsFileConfigurator.parseOrGetDefaultConvertOnTypeMismatch(loadAttributes);
    this.tabletConversionThresholdBytes =
        LoadTsFileConfigurator.parseOrGetDefaultTabletConversionThresholdBytes(loadAttributes);
    this.verify = LoadTsFileConfigurator.parseOrGetDefaultVerify(loadAttributes);
    this.isAsyncLoad = LoadTsFileConfigurator.parseOrGetDefaultAsyncLoad(loadAttributes);
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
   * Splits the current LoadTsFile statement into multiple sub-statements, each handling a batch of
   * TsFiles. Used to limit resource consumption during statement analysis, etc.
   *
   * @return the list of sub-statements
   */
  @Override
  public List<LoadTsFile> getSubStatements() {
    final int batchSize =
        IoTDBDescriptor.getInstance().getConfig().getLoadTsFileSubStatementBatchSize();
    final int totalBatches = (tsFiles.size() + batchSize - 1) / batchSize; // Ceiling division
    final List<LoadTsFile> subStatements = new ArrayList<>(totalBatches);

    for (int i = 0; i < tsFiles.size(); i += batchSize) {
      final int endIndex = Math.min(i + batchSize, tsFiles.size());
      final List<File> batchFiles = tsFiles.subList(i, endIndex);

      // Use the first file's path for the sub-statement
      final String filePath = batchFiles.get(0).getAbsolutePath();
      final Map<String, String> properties = this.loadAttributes;

      final LoadTsFile subStatement =
          new LoadTsFile(getLocation().orElse(null), filePath, properties);

      // Copy all configuration properties
      subStatement.databaseLevel = this.databaseLevel;
      subStatement.database = this.database;
      subStatement.verify = this.verify;
      subStatement.deleteAfterLoad = this.deleteAfterLoad;
      subStatement.convertOnTypeMismatch = this.convertOnTypeMismatch;
      subStatement.tabletConversionThresholdBytes = this.tabletConversionThresholdBytes;
      subStatement.autoCreateDatabase = this.autoCreateDatabase;
      subStatement.isAsyncLoad = this.isAsyncLoad;
      subStatement.isGeneratedByPipe = this.isGeneratedByPipe;

      // Set all files in the batch
      subStatement.tsFiles = new ArrayList<>(batchFiles);
      subStatement.resources = new ArrayList<>(batchFiles.size());
      subStatement.writePointCountList = new ArrayList<>(batchFiles.size());
      subStatement.isTableModel = new ArrayList<>(batchFiles.size());
      for (int j = 0; j < batchFiles.size(); j++) {
        subStatement.isTableModel.add(true);
      }

      subStatements.add(subStatement);
    }

    return subStatements;
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

  @Override
  public long ramBytesUsed() {
    long size = INSTANCE_SIZE;
    size += AstMemoryEstimationHelper.getEstimatedSizeOfNodeLocation(getLocationInternal());
    size += RamUsageEstimator.sizeOf(filePath);
    size += RamUsageEstimator.sizeOf(database);
    size += RamUsageEstimator.sizeOfMap(loadAttributes);
    if (tsFiles != null) {
      size += RamUsageEstimator.shallowSizeOf(tsFiles);
      for (File file : tsFiles) {
        if (file != null) {
          size += FILE_INSTANCE_SIZE;
        }
      }
    }
    if (resources != null) {
      size += RamUsageEstimator.shallowSizeOf(resources);
      for (TsFileResource resource : resources) {
        if (resource != null) {
          size += resource.calculateRamSize();
        }
      }
    }
    size += AstMemoryEstimationHelper.getEstimatedSizeOfLongList(writePointCountList);
    size += RamUsageEstimator.shallowSizeOf(isTableModel);
    return size;
  }
}
