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

package org.apache.iotdb.db.queryengine.plan.analyze.load;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.LoadEmptyFileException;
import org.apache.iotdb.db.exception.sql.SemanticException;
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.IAnalysis;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.EncryptUtils;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class LoadTsFileToTreeModelAnalyzer extends LoadTsFileAnalyzer {
  private static final Logger LOGGER = LoggerFactory.getLogger(LoadTsFileToTreeModelAnalyzer.class);

  private final TreeSchemaAutoCreatorAndVerifier schemaAutoCreatorAndVerifier;

  public LoadTsFileToTreeModelAnalyzer(
      LoadTsFileStatement loadTsFileStatement, MPPQueryContext context) {
    super(loadTsFileStatement, context);
    this.schemaAutoCreatorAndVerifier = new TreeSchemaAutoCreatorAndVerifier(this);
  }

  public LoadTsFileToTreeModelAnalyzer(
      LoadTsFile loadTsFileTableStatement, MPPQueryContext context) {
    super(loadTsFileTableStatement, context);
    this.schemaAutoCreatorAndVerifier = new TreeSchemaAutoCreatorAndVerifier(this);
  }

  @Override
  public IAnalysis analyzeFileByFile(IAnalysis analysis) {
    checkBeforeAnalyzeFileByFile(analysis);
    if (analysis.isFinishQueryAfterAnalyze()) {
      return analysis;
    }

    if (!doAnalyzeFileByFile(analysis)) {
      // return false means the analysis is failed because of exception
      return analysis;
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
  protected void analyzeSingleTsFile(final File tsFile) throws IOException, AuthException {
    try (final TsFileSequenceReader reader = new TsFileSequenceReader(tsFile.getAbsolutePath())) {
      // can be reused when constructing tsfile resource
      final TsFileSequenceReaderTimeseriesMetadataIterator timeseriesMetadataIterator =
          new TsFileSequenceReaderTimeseriesMetadataIterator(reader, true, 1);

      // check if the tsfile is empty
      if (!timeseriesMetadataIterator.hasNext()) {
        throw new LoadEmptyFileException(tsFile.getAbsolutePath());
      }

      // check whether the encrypt type of the tsfile is supported
      EncryptParameter param = reader.getEncryptParam();
      if (!Objects.equals(param.getType(), EncryptUtils.encryptParam.getType())
          || !Arrays.equals(param.getKey(), EncryptUtils.encryptParam.getKey())) {
        throw new SemanticException("The encryption way of the TsFile is not supported.");
      }

      // check whether the tsfile is tree-model or not
      // TODO: currently, loading a file with both tree-model and table-model data is not supported.
      //  May need to support this and remove this check in the future.
      if (Objects.nonNull(reader.readFileMetadata().getTableSchemaMap())
          && reader.readFileMetadata().getTableSchemaMap().size() != 0) {
        throw new SemanticException("Attempted to load a table-model TsFile into tree-model.");
      }

      // construct tsfile resource
      final TsFileResource tsFileResource = constructTsFileResource(reader, tsFile);

      schemaAutoCreatorAndVerifier.setCurrentModificationsAndTimeIndex(tsFileResource, reader);

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
      if (isDeleteAfterLoad) {
        FileUtils.deleteQuietly(tsFile);
      }
    }
  }

  @Override
  public void close() throws Exception {
    schemaAutoCreatorAndVerifier.close();
  }
}
