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
import org.apache.iotdb.db.queryengine.common.MPPQueryContext;
import org.apache.iotdb.db.queryengine.plan.analyze.IPartitionFetcher;
import org.apache.iotdb.db.queryengine.plan.analyze.schema.ISchemaFetcher;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.ITableDeviceSchemaValidation;
import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.LoadTsFile;
import org.apache.iotdb.db.queryengine.plan.statement.crud.LoadTsFileStatement;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResourceStatus;
import org.apache.iotdb.db.storageengine.dataregion.utils.TsFileResourceUtils;
import org.apache.iotdb.db.utils.TimestampPrecisionUtils;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.TsFileSequenceReaderTimeseriesMetadataIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class LoadTsFileToTableModelAnalyzer extends LoadTsFileAnalyzer {
  private static final Logger LOGGER =
      LoggerFactory.getLogger(LoadTsFileToTableModelAnalyzer.class);

  public LoadTsFileToTableModelAnalyzer(
      LoadTsFileStatement loadTsFileStatement,
      MPPQueryContext context,
      IPartitionFetcher partitionFetcher,
      ISchemaFetcher schemaFetcher) {
    super(loadTsFileStatement, context, partitionFetcher, schemaFetcher);
  }

  public LoadTsFileToTableModelAnalyzer(
      LoadTsFile loadTsFileTableStatement, Metadata metadata, MPPQueryContext context) {
    super(loadTsFileTableStatement, metadata, context);
  }

  @Override
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

      LOGGER.error("TableSchemaMap size: {}", reader.readFileMetadata().getTableSchemaMap().size());
      for (Map.Entry<String, TableSchema> name2Schema :
          reader.readFileMetadata().getTableSchemaMap().entrySet()) {
        LOGGER.error("validating table header: {}", name2Schema.getKey());
        org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema realSchema =
            metadata
                .validateTableHeaderSchema(
                    getDatabase(),
                    org.apache.iotdb.db.queryengine.plan.relational.metadata.TableSchema
                        .fromTsFileTableSchema(name2Schema.getKey(), name2Schema.getValue()),
                    context,
                    true)
                .orElse(null);
        if (Objects.isNull(realSchema)) {
          LOGGER.warn("Failed to validata schema for table {}", name2Schema.getValue());
        }
      }

      long writePointCount = 0;

      final boolean isAutoCreateSchemaOrVerifySchemaEnabled =
          IoTDBDescriptor.getInstance().getConfig().isAutoCreateSchemaEnabled() || isVerifySchema();
      while (timeseriesMetadataIterator.hasNext()) {
        final Map<IDeviceID, List<TimeseriesMetadata>> device2TimeseriesMetadata =
            timeseriesMetadataIterator.next();

        if (isAutoCreateSchemaOrVerifySchemaEnabled) {
          for (IDeviceID deviceId : device2TimeseriesMetadata.keySet()) {
            final ITableDeviceSchemaValidation tableSchemaValidation =
                createTableSchemaValidation(deviceId, this);
            metadata.validateDeviceSchema(tableSchemaValidation, context);
          }
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

  private ITableDeviceSchemaValidation createTableSchemaValidation(
      IDeviceID deviceId, LoadTsFileToTableModelAnalyzer analyzer) {
    return new ITableDeviceSchemaValidation() {

      @Override
      public String getDatabase() {
        return analyzer.getDatabase();
      }

      @Override
      public String getTableName() {
        return deviceId.getTableName();
      }

      @Override
      public List<Object[]> getDeviceIdList() {
        return Collections.singletonList(
            Arrays.copyOfRange(deviceId.getSegments(), 1, deviceId.getSegments().length));
      }

      @Override
      public List<String> getAttributeColumnNameList() {
        return Collections.emptyList();
      }

      @Override
      public List<Object[]> getAttributeValueList() {
        return Collections.singletonList(new Object[0]);
      }
    };
  }
}
