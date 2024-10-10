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

package org.apache.iotdb.db.queryengine.plan.analyze.load;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.LoadEmptyFileException;
import org.apache.iotdb.db.exception.LoadReadOnlyException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.ClusterPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ClusterSchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.BufferUnderflowException;
import java.util.List;
import java.util.Map;

public abstract class LoadTsFileAnalyzer implements AutoCloseable {

  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileAnalyzer.class);

  // These are only used when constructed from tree model SQL
  private final LoadTsFileStatement loadTsFileStatement;

  // These are only used when constructed from table model SQL
  private final LoadTsFile loadTsFileTableStatement;
  protected final Metadata metadata;

  private final boolean isTableModelStatement;

  final MPPQueryContext context;

  final IPartitionFetcher partitionFetcher;
  final ISchemaFetcher schemaFetcher;

  protected final SchemaAutoCreatorAndVerifier schemaAutoCreatorAndVerifier;

  LoadTsFileAnalyzer(
      LoadTsFileStatement loadTsFileStatement,
      MPPQueryContext context,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher) {
    this.loadTsFileStatement = loadTsFileStatement;

    this.loadTsFileTableStatement = null;
    this.metadata = null;

    this.isTableModelStatement = false;

    this.context = context;

    this.partitionFetcher = partitionFetcher;
    this.schemaFetcher = schemaFetcher;

    this.schemaAutoCreatorAndVerifier = new SchemaAutoCreatorAndVerifier(this);
  }

  LoadTsFileAnalyzer(
      LoadTsFile loadTsFileTableStatement, Metadata metadata, MPPQueryContext context) {
    this.loadTsFileStatement = null;

    this.loadTsFileTableStatement = loadTsFileTableStatement;
    this.metadata = metadata;

    this.isTableModelStatement = true;

    this.context = context;

    this.partitionFetcher = ClusterPartitionFetcher.getInstance();
    this.schemaFetcher = ClusterSchemaFetcher.getInstance();

    this.schemaAutoCreatorAndVerifier = new SchemaAutoCreatorAndVerifier(this);
  }

  public IAnalysis analyzeFileByFile(IAnalysis analysis) {
    // check if the system is read only
    if (CommonDescriptor.getInstance().getConfig().isReadOnly()) {
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(
          RpcUtils.getStatus(TSStatusCode.SYSTEM_READ_ONLY, LoadReadOnlyException.MESSAGE));
      return analysis;
    }

    // analyze tsfile metadata file by file
    for (int i = 0, tsfileNum = getTsFiles().size(); i < tsfileNum; i++) {
      final File tsFile = getTsFiles().get(i);

      if (tsFile.length() == 0) {
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn("TsFile {} is empty.", tsFile.getPath());
        }
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Load - Analysis Stage: {}/{} tsfiles have been analyzed, progress: {}%",
              i + 1, tsfileNum, String.format("%.3f", (i + 1) * 100.00 / tsfileNum));
        }
        continue;
      }

      try {
        analyzeSingleTsFile(tsFile);
        if (LOGGER.isInfoEnabled()) {
          LOGGER.info(
              "Load - Analysis Stage: {}/{} tsfiles have been analyzed, progress: {}%",
              i + 1, tsfileNum, String.format("%.3f", (i + 1) * 100.00 / tsfileNum));
        }
      } catch (AuthException e) {
        setFailAnalysisForAuthException(analysis, e);
        return analysis;
      } catch (BufferUnderflowException e) {
        LOGGER.warn(
            "The file {} is not a valid tsfile. Please check the input file.", tsFile.getPath(), e);
        throw new SemanticException(
            String.format(
                "The file %s is not a valid tsfile. Please check the input file.",
                tsFile.getPath()));
      } catch (Exception e) {
        final String exceptionMessage =
            String.format(
                "The file %s is not a valid tsfile. Please check the input file. Detail: %s",
                tsFile.getPath(), e.getMessage() == null ? e.getClass().getName() : e.getMessage());
        LOGGER.warn(exceptionMessage, e);
        analysis.setFinishQueryAfterAnalyze(true);
        analysis.setFailStatus(RpcUtils.getStatus(TSStatusCode.LOAD_FILE_ERROR, exceptionMessage));
        return analysis;
      }
    }

    try {
      schemaAutoCreatorAndVerifier.flush();
    } catch (AuthException e) {
      setFailAnalysisForAuthException(analysis, e);
      return analysis;
    } catch (Exception e) {
      final String exceptionMessage =
          String.format(
              "Auto create or verify schema error when executing statement %s. Detail: %s.",
              getStatementString(),
              e.getMessage() == null ? e.getClass().getName() : e.getMessage());
      LOGGER.warn(exceptionMessage, e);
      analysis.setFinishQueryAfterAnalyze(true);
      analysis.setFailStatus(RpcUtils.getStatus(TSStatusCode.LOAD_FILE_ERROR, exceptionMessage));
      return analysis;
    }

    LOGGER.info("Load - Analysis Stage: all tsfiles have been analyzed.");

    // data partition will be queried in the scheduler
    setRealStatement(analysis);
    return analysis;
  }

  @Override
  public void close() {
    schemaAutoCreatorAndVerifier.close();
  }

  protected void analyzeSingleTsFile(final File tsFile) throws IOException, AuthException {
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      // can be reused when constructing tsfile resource
      final TsFileSequenceReaderTimeseriesMetadataIterator timeseriesMetadataIterator =
          new TsFileSequenceReaderTimeseriesMetadataIterator(reader, true, 1);

      // construct tsfile resource
      final TsFileResource tsFileResource = new TsFileResource(tsFile);
      if (!tsFileResource.resourceFileExists()) {
        // it will be serialized in LoadSingleTsFileNode
        tsFileResource.updatePlanIndexes(reader.getMinPlanIndex());
        tsFileResource.updatePlanIndexes(reader.getMaxPlanIndex());
      } else {
        tsFileResource.deserialize();
      }

      schemaAutoCreatorAndVerifier.setCurrentModificationsAndTimeIndex(tsFileResource, reader);

      // check if the tsfile is empty
      if (!timeseriesMetadataIterator.hasNext()) {
        throw new LoadEmptyFileException(tsFile.getAbsolutePath());
      }

      long writePointCount = 0;

      final boolean isAutoCreateSchemaOrVerifySchemaEnabled =
          IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled() || isVerifySchema();
      while (timeseriesMetadataIterator.hasNext()) {
        final Map<IDeviceID, List<TimeseriesMetadata>> device2TimeseriesMetadata =
            timeseriesMetadataIterator.next();

        if (isAutoCreateSchemaOrVerifySchemaEnabled) {
          schemaAutoCreatorAndVerifier.autoCreateAndVerify(reader, device2TimeseriesMetadata);
        }

        if (!tsFileResource.resourceFileExists()) {
          TsFileResourceUtils.updateTsFileResource(device2TimeseriesMetadata, tsFileResource);
        }

        // TODO: how to get the correct write point count when
        //  !isAutoCreateSchemaOrVerifySchemaEnabled
        writePointCount += getWritePointCount(device2TimeseriesMetadata);
      }
      if (isAutoCreateSchemaOrVerifySchemaEnabled) {
        schemaAutoCreatorAndVerifier.flushAndClearDeviceIsAlignedCacheIfNecessary();
      }

      TimestampPrecisionUtils.checkTimestampPrecision(tsFileResource.getFileEndTime());
      tsFileResource.setStatus(TsFileResourceStatus.NORMAL);

      addTsFileResource(tsFileResource);
      addWritePointCount(writePointCount);
    } catch (final LoadEmptyFileException loadEmptyFileException) {
      LOGGER.warn("Failed to load empty file: {}", tsFile.getAbsolutePath());
      if (isDeleteAfterLoad()) {
        FileUtils.deleteQuietly(tsFile);
      }
    }
  }

  protected List<File> getTsFiles() {
    if (isTableModelStatement) {
      return loadTsFileTableStatement.getTsFiles();
    } else {
      return loadTsFileStatement.getTsFiles();
    }
  }

  protected String getStatementString() {
    if (isTableModelStatement) {
      return loadTsFileTableStatement.toString();
    } else {
      return loadTsFileStatement.toString();
    }
  }

  protected void setRealStatement(IAnalysis analysis) {
    if (isTableModelStatement) {
      // Do nothing by now.
    } else {
      analysis.setRealStatement(loadTsFileStatement);
    }
  }

  protected void addTsFileResource(TsFileResource tsFileResource) {
    if (isTableModelStatement) {
      loadTsFileTableStatement.addTsFileResource(tsFileResource);
    } else {
      loadTsFileStatement.addTsFileResource(tsFileResource);
    }
  }

  protected void addWritePointCount(long writePointCount) {
    if (isTableModelStatement) {
      loadTsFileTableStatement.addWritePointCount(writePointCount);
    } else {
      loadTsFileStatement.addWritePointCount(writePointCount);
    }
  }

  protected boolean isVerifySchema() {
    if (isTableModelStatement) {
      return true;
    } else {
      return loadTsFileStatement.isVerifySchema();
    }
  }

  protected boolean isDeleteAfterLoad() {
    if (isTableModelStatement) {
      return loadTsFileTableStatement.isDeleteAfterLoad();
    } else {
      return loadTsFileStatement.isDeleteAfterLoad();
    }
  }

  protected boolean isAutoCreateDatabase() {
    if (isTableModelStatement) {
      return loadTsFileTableStatement.isAutoCreateDatabase();
    } else {
      return loadTsFileStatement.isAutoCreateDatabase();
    }
  }

  protected int getDatabaseLevel() {
    if (isTableModelStatement) {
      return loadTsFileTableStatement.getDatabaseLevel();
    } else {
      return loadTsFileStatement.getDatabaseLevel();
    }
  }

  protected @Nullable String getDatabase() {
    if (isTableModelStatement) {
      return loadTsFileTableStatement.getDatabase();
    } else {
      return loadTsFileStatement.getDatabase();
    }
  }

  protected long getWritePointCount(
      Map<IDeviceID, List<TimeseriesMetadata>> device2TimeseriesMetadata) {
    return device2TimeseriesMetadata.values().stream()
        .flatMap(List::stream)
        .mapToLong(t -> t.getStatistics().getCount())
        .sum();
  }

  protected void setFailAnalysisForAuthException(IAnalysis analysis, AuthException e) {
    analysis.setFinishQueryAfterAnalyze(true);
    analysis.setFailStatus(RpcUtils.getStatus(e.getCode(), e.getMessage()));
  }
}
