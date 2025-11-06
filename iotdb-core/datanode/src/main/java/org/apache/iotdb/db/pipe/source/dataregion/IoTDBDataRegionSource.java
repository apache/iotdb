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

package org.apache.iotdb.db.pipe.source.dataregion;

import org.apache.iotdb.commons.consensus.DataRegionId;
import org.apache.iotdb.commons.pipe.agent.task.PipeTaskAgent;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.commons.pipe.datastructure.pattern.IoTDBTreePatternOperations;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.commons.pipe.source.IoTDBSource;
import org.apache.iotdb.consensus.ConsensusFactory;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.pipe.event.common.heartbeat.PipeHeartbeatEvent;
import org.apache.iotdb.db.pipe.metric.overview.PipeDataNodeSinglePipeMetrics;
import org.apache.iotdb.db.pipe.metric.overview.PipeTsFileToTabletsMetrics;
import org.apache.iotdb.db.pipe.metric.source.PipeDataRegionSourceMetrics;
import org.apache.iotdb.db.pipe.source.dataregion.historical.PipeHistoricalDataRegionSource;
import org.apache.iotdb.db.pipe.source.dataregion.historical.PipeHistoricalDataRegionTsFileAndDeletionSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionHeartbeatSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionHybridSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionLogSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionSource;
import org.apache.iotdb.db.pipe.source.dataregion.realtime.PipeRealtimeDataRegionTsFileSource;
import org.apache.iotdb.db.storageengine.StorageEngine;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;
import org.apache.iotdb.pipe.api.event.dml.insertion.TabletInsertionEvent;
import org.apache.iotdb.pipe.api.event.dml.insertion.TsFileInsertionEvent;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_DATABASE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_DATABASE_NAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_HISTORY_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_HISTORY_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_HISTORY_LOOSE_RANGE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_HISTORY_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_MODE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_MODE_SNAPSHOT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_MODE_STREAMING_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_MODE_STREAMING_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_MODE_STRICT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_MODS_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_MODS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATTERN_FORMAT_IOTDB_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATTERN_FORMAT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATTERN_FORMAT_PREFIX_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_PATTERN_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_REALTIME_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_REALTIME_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_REALTIME_LOOSE_RANGE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_REALTIME_MODE_BATCH_MODE_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_REALTIME_MODE_FILE_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_REALTIME_MODE_FORCED_LOG_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_REALTIME_MODE_HYBRID_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_REALTIME_MODE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_REALTIME_MODE_LOG_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_REALTIME_MODE_STREAM_MODE_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_TABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_TABLE_NAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_WATERMARK_INTERVAL_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_WATERMARK_INTERVAL_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_DATABASE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_DATABASE_NAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_HISTORY_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_HISTORY_END_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_HISTORY_LOOSE_RANGE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_HISTORY_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_MODE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_MODE_SNAPSHOT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_MODE_STREAMING_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_MODE_STRICT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_MODS_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_MODS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_PATH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_PATTERN_FORMAT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_PATTERN_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_REALTIME_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_REALTIME_LOOSE_RANGE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_REALTIME_MODE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_START_TIME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_TABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_TABLE_NAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_WATERMARK_INTERVAL_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant._EXTRACTOR_WATERMARK_INTERVAL_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant._SOURCE_WATERMARK_INTERVAL_KEY;

@TreeModel
@TableModel
public class IoTDBDataRegionSource extends IoTDBSource {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBDataRegionSource.class);

  private PipeHistoricalDataRegionSource historicalExtractor;
  private PipeRealtimeDataRegionSource realtimeExtractor;

  private DataRegionWatermarkInjector watermarkInjector;

  private boolean hasNoExtractionNeed = true;
  private boolean shouldExtractDeletion = false;

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    super.validate(validator);

    final boolean isTreeDialect =
        validator
            .getParameters()
            .getStringOrDefault(
                SystemConstant.SQL_DIALECT_KEY, SystemConstant.SQL_DIALECT_TREE_VALUE)
            .equals(SystemConstant.SQL_DIALECT_TREE_VALUE);
    // Validate whether the pipe needs to extract table model data or tree model data
    final boolean isCaptureTree =
        validator
            .getParameters()
            .getBooleanOrDefault(
                Arrays.asList(
                    PipeSourceConstant.EXTRACTOR_CAPTURE_TREE_KEY,
                    PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY),
                isTreeDialect);
    final boolean isCaptureTable =
        validator
            .getParameters()
            .getBooleanOrDefault(
                Arrays.asList(
                    PipeSourceConstant.EXTRACTOR_CAPTURE_TABLE_KEY,
                    PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY),
                !isTreeDialect);
    if (!isCaptureTree && !isCaptureTable) {
      throw new PipeParameterNotValidException(
          "capture.tree and capture.table can not both be specified as false");
    }

    final boolean isDoubleLiving =
        validator
            .getParameters()
            .getBooleanOrDefault(
                Arrays.asList(
                    PipeSourceConstant.EXTRACTOR_MODE_DOUBLE_LIVING_KEY,
                    PipeSourceConstant.SOURCE_MODE_DOUBLE_LIVING_KEY),
                PipeSourceConstant.EXTRACTOR_MODE_DOUBLE_LIVING_DEFAULT_VALUE);
    final boolean isTreeModelDataAllowedToBeCaptured = isDoubleLiving || isCaptureTree;
    final boolean isTableModelDataAllowedToBeCaptured = isDoubleLiving || isCaptureTable;
    if (!isTreeModelDataAllowedToBeCaptured
        && validator
            .getParameters()
            .hasAnyAttributes(
                EXTRACTOR_PATH_KEY, SOURCE_PATH_KEY, EXTRACTOR_PATTERN_KEY, SOURCE_PATTERN_KEY)) {
      throw new PipeException(
          "The pipe cannot extract tree model data when sql dialect is set to table.");
    }
    if (!isTableModelDataAllowedToBeCaptured
        && validator
            .getParameters()
            .hasAnyAttributes(
                EXTRACTOR_DATABASE_NAME_KEY,
                SOURCE_DATABASE_NAME_KEY,
                EXTRACTOR_TABLE_NAME_KEY,
                SOURCE_TABLE_NAME_KEY,
                EXTRACTOR_DATABASE_KEY,
                SOURCE_DATABASE_KEY,
                EXTRACTOR_TABLE_KEY,
                SOURCE_TABLE_KEY)) {
      throw new PipeException(
          "The pipe cannot extract table model data when sql dialect is set to tree.");
    }

    final Pair<Boolean, Boolean> insertionDeletionListeningOptionPair =
        DataRegionListeningFilter.parseInsertionDeletionListeningOptionPair(
            validator.getParameters());
    if (insertionDeletionListeningOptionPair.getLeft().equals(false)
        && insertionDeletionListeningOptionPair.getRight().equals(false)) {
      return;
    }
    hasNoExtractionNeed = false;
    shouldExtractDeletion = insertionDeletionListeningOptionPair.getRight();

    if (insertionDeletionListeningOptionPair.getLeft().equals(true)
        && IoTDBDescriptor.getInstance()
            .getConfig()
            .getDataRegionConsensusProtocolClass()
            .equals(ConsensusFactory.RATIS_CONSENSUS)) {
      throw new PipeException(
          "The pipe cannot transfer data when data region is using ratis consensus.");
    }

    // Validate source.pattern.format is within valid range
    validator
        .validateAttributeValueRange(
            EXTRACTOR_PATTERN_FORMAT_KEY,
            true,
            EXTRACTOR_PATTERN_FORMAT_PREFIX_VALUE,
            EXTRACTOR_PATTERN_FORMAT_IOTDB_VALUE)
        .validateAttributeValueRange(
            SOURCE_PATTERN_FORMAT_KEY,
            true,
            EXTRACTOR_PATTERN_FORMAT_PREFIX_VALUE,
            EXTRACTOR_PATTERN_FORMAT_IOTDB_VALUE);

    // Validate tree pattern and table pattern
    validatePattern(TreePattern.parsePipePatternFromSourceParameters(validator.getParameters()));

    // Validate source.history.enable and source.realtime.enable
    validator
        .validateAttributeValueRange(
            EXTRACTOR_HISTORY_ENABLE_KEY, true, Boolean.TRUE.toString(), Boolean.FALSE.toString())
        .validateAttributeValueRange(
            EXTRACTOR_REALTIME_ENABLE_KEY, true, Boolean.TRUE.toString(), Boolean.FALSE.toString())
        .validateAttributeValueRange(
            SOURCE_HISTORY_ENABLE_KEY, true, Boolean.TRUE.toString(), Boolean.FALSE.toString())
        .validateAttributeValueRange(
            SOURCE_REALTIME_ENABLE_KEY, true, Boolean.TRUE.toString(), Boolean.FALSE.toString())
        .validate(
            args -> (boolean) args[0] || (boolean) args[1],
            "Should not set both history.enable and realtime.enable to false.",
            validator
                .getParameters()
                .getBooleanOrDefault(
                    Arrays.asList(EXTRACTOR_HISTORY_ENABLE_KEY, SOURCE_HISTORY_ENABLE_KEY),
                    EXTRACTOR_HISTORY_ENABLE_DEFAULT_VALUE),
            validator
                .getParameters()
                .getBooleanOrDefault(
                    Arrays.asList(EXTRACTOR_REALTIME_ENABLE_KEY, SOURCE_REALTIME_ENABLE_KEY),
                    EXTRACTOR_REALTIME_ENABLE_DEFAULT_VALUE));

    // Validate source.realtime.mode
    if (validator
            .getParameters()
            .getBooleanOrDefault(
                Arrays.asList(EXTRACTOR_REALTIME_ENABLE_KEY, SOURCE_REALTIME_ENABLE_KEY),
                EXTRACTOR_REALTIME_ENABLE_DEFAULT_VALUE)
        || validator
            .getParameters()
            .hasAnyAttributes(
                SOURCE_START_TIME_KEY,
                EXTRACTOR_START_TIME_KEY,
                SOURCE_END_TIME_KEY,
                EXTRACTOR_END_TIME_KEY)) {
      validator.validateAttributeValueRange(
          validator.getParameters().hasAttribute(EXTRACTOR_REALTIME_MODE_KEY)
              ? EXTRACTOR_REALTIME_MODE_KEY
              : SOURCE_REALTIME_MODE_KEY,
          true,
          EXTRACTOR_REALTIME_MODE_FILE_VALUE,
          EXTRACTOR_REALTIME_MODE_HYBRID_VALUE,
          EXTRACTOR_REALTIME_MODE_LOG_VALUE,
          EXTRACTOR_REALTIME_MODE_FORCED_LOG_VALUE,
          EXTRACTOR_REALTIME_MODE_STREAM_MODE_VALUE,
          EXTRACTOR_REALTIME_MODE_BATCH_MODE_VALUE);
    }

    checkInvalidParameters(validator);

    constructHistoricalExtractor();
    constructRealtimeExtractor(validator.getParameters());

    historicalExtractor.validate(validator);
    realtimeExtractor.validate(validator);
  }

  private void validatePattern(final TreePattern treePattern) {
    if (!treePattern.isLegal()) {
      throw new IllegalArgumentException(String.format("Pattern \"%s\" is illegal.", treePattern));
    }

    if (shouldExtractDeletion
        && !(treePattern instanceof IoTDBTreePatternOperations
            && (((IoTDBTreePatternOperations) treePattern).isPrefixOrFullPath()))) {
      throw new IllegalArgumentException(
          String.format(
              "The path pattern %s is not valid for the source. Only prefix or full path is allowed.",
              treePattern));
    }
  }

  private void checkInvalidParameters(final PipeParameterValidator validator) {
    final PipeParameters parameters = validator.getParameters();

    // Enable history and realtime if specifying start-time or end-time
    if (parameters.hasAnyAttributes(
            SOURCE_START_TIME_KEY,
            EXTRACTOR_START_TIME_KEY,
            SOURCE_END_TIME_KEY,
            EXTRACTOR_END_TIME_KEY)
        && parameters.hasAnyAttributes(
            EXTRACTOR_HISTORY_ENABLE_KEY,
            SOURCE_HISTORY_ENABLE_KEY,
            SOURCE_HISTORY_START_TIME_KEY,
            EXTRACTOR_HISTORY_START_TIME_KEY,
            SOURCE_HISTORY_END_TIME_KEY,
            EXTRACTOR_HISTORY_END_TIME_KEY)) {
      LOGGER.warn(
          "When {}, {}, {} or {} is specified, specifying {}, {}, {}, {}, {} and {} is invalid.",
          SOURCE_START_TIME_KEY,
          EXTRACTOR_START_TIME_KEY,
          SOURCE_END_TIME_KEY,
          EXTRACTOR_END_TIME_KEY,
          SOURCE_HISTORY_ENABLE_KEY,
          EXTRACTOR_HISTORY_ENABLE_KEY,
          SOURCE_HISTORY_START_TIME_KEY,
          EXTRACTOR_HISTORY_START_TIME_KEY,
          SOURCE_HISTORY_END_TIME_KEY,
          EXTRACTOR_HISTORY_END_TIME_KEY);
    }

    // Check coexistence of database-name and database
    validator.validateSynonymAttributes(
        Arrays.asList(EXTRACTOR_DATABASE_NAME_KEY, SOURCE_DATABASE_NAME_KEY),
        Arrays.asList(EXTRACTOR_DATABASE_KEY, SOURCE_DATABASE_KEY),
        false);

    // Check coexistence of table-name and table
    validator.validateSynonymAttributes(
        Arrays.asList(EXTRACTOR_TABLE_NAME_KEY, SOURCE_TABLE_NAME_KEY),
        Arrays.asList(EXTRACTOR_TABLE_KEY, SOURCE_TABLE_KEY),
        false);

    // Check coexistence of mode.snapshot and mode
    validator.validateSynonymAttributes(
        Arrays.asList(EXTRACTOR_MODE_SNAPSHOT_KEY, SOURCE_MODE_SNAPSHOT_KEY),
        Arrays.asList(EXTRACTOR_MODE_KEY, SOURCE_MODE_KEY),
        false);

    // Check coexistence of mode.streaming and realtime.mode
    validator.validateSynonymAttributes(
        Arrays.asList(EXTRACTOR_MODE_STREAMING_KEY, SOURCE_MODE_STREAMING_KEY),
        Arrays.asList(EXTRACTOR_REALTIME_MODE_KEY, SOURCE_REALTIME_MODE_KEY),
        false);

    // Check coexistence of mode.strict, history.loose-range and realtime.loose-range
    validator.validateSynonymAttributes(
        Arrays.asList(EXTRACTOR_MODE_STRICT_KEY, SOURCE_MODE_STRICT_KEY),
        Arrays.asList(
            EXTRACTOR_HISTORY_LOOSE_RANGE_KEY,
            SOURCE_HISTORY_LOOSE_RANGE_KEY,
            EXTRACTOR_REALTIME_LOOSE_RANGE_KEY,
            SOURCE_REALTIME_LOOSE_RANGE_KEY),
        false);

    // Check coexistence of mods and mods.enable
    validator.validateSynonymAttributes(
        Arrays.asList(EXTRACTOR_MODS_ENABLE_KEY, SOURCE_MODS_ENABLE_KEY),
        Arrays.asList(EXTRACTOR_MODS_KEY, SOURCE_MODS_KEY),
        false);

    // Check coexistence of watermark.interval-ms and watermark-interval-ms
    validator.validateSynonymAttributes(
        Arrays.asList(EXTRACTOR_WATERMARK_INTERVAL_KEY, SOURCE_WATERMARK_INTERVAL_KEY),
        Arrays.asList(_EXTRACTOR_WATERMARK_INTERVAL_KEY, _SOURCE_WATERMARK_INTERVAL_KEY),
        false);

    // Check if specifying mode.snapshot or mode.streaming when disable realtime source
    if (!parameters.getBooleanOrDefault(
        Arrays.asList(EXTRACTOR_REALTIME_ENABLE_KEY, SOURCE_REALTIME_ENABLE_KEY),
        EXTRACTOR_REALTIME_ENABLE_DEFAULT_VALUE)) {
      if (parameters.hasAnyAttributes(EXTRACTOR_REALTIME_MODE_KEY, SOURCE_REALTIME_MODE_KEY)) {
        LOGGER.warn(
            "When '{}' ('{}') is set to false, specifying {} and {} is invalid.",
            EXTRACTOR_REALTIME_ENABLE_KEY,
            SOURCE_REALTIME_ENABLE_KEY,
            EXTRACTOR_REALTIME_MODE_KEY,
            SOURCE_REALTIME_MODE_KEY);
      }
      if (parameters.hasAnyAttributes(EXTRACTOR_MODE_STREAMING_KEY, SOURCE_MODE_STREAMING_KEY)) {
        LOGGER.warn(
            "When '{}' ('{}') is set to false, specifying {} and {} is invalid.",
            EXTRACTOR_REALTIME_ENABLE_KEY,
            SOURCE_REALTIME_ENABLE_KEY,
            EXTRACTOR_MODE_STREAMING_KEY,
            SOURCE_MODE_STREAMING_KEY);
      }
    } else {
      if (parameters.hasAnyAttributes(
          EXTRACTOR_MODE_SNAPSHOT_KEY,
          SOURCE_MODE_SNAPSHOT_KEY,
          EXTRACTOR_MODE_KEY,
          SOURCE_MODE_KEY)) {
        LOGGER.warn(
            "When '{}' ('{}', '{}', '{}') is set to true, specifying {} and {} is invalid.",
            EXTRACTOR_MODE_SNAPSHOT_KEY,
            SOURCE_MODE_SNAPSHOT_KEY,
            EXTRACTOR_MODE_KEY,
            SOURCE_MODE_KEY,
            EXTRACTOR_REALTIME_ENABLE_KEY,
            SOURCE_REALTIME_ENABLE_KEY);
      }
    }
  }

  private void constructHistoricalExtractor() {
    historicalExtractor = new PipeHistoricalDataRegionTsFileAndDeletionSource();
  }

  private void constructRealtimeExtractor(final PipeParameters parameters) {
    // Use heartbeat only source if disable realtime source
    if (!parameters.getBooleanOrDefault(
        Arrays.asList(EXTRACTOR_REALTIME_ENABLE_KEY, SOURCE_REALTIME_ENABLE_KEY),
        EXTRACTOR_REALTIME_ENABLE_DEFAULT_VALUE)) {
      realtimeExtractor = new PipeRealtimeDataRegionHeartbeatSource();
      LOGGER.info(
          "Pipe: '{}' ('{}') is set to false, use heartbeat realtime source.",
          EXTRACTOR_REALTIME_ENABLE_KEY,
          SOURCE_REALTIME_ENABLE_KEY);
      return;
    }

    // Use heartbeat only source if enable snapshot mode
    if (PipeTaskAgent.isSnapshotMode(parameters)) {
      realtimeExtractor = new PipeRealtimeDataRegionHeartbeatSource();
      LOGGER.info("Pipe: snapshot mode is enabled, use heartbeat realtime source.");
      return;
    }

    // Use hybrid mode by default
    if (!parameters.hasAnyAttributes(EXTRACTOR_MODE_STREAMING_KEY, SOURCE_MODE_STREAMING_KEY)
        && !parameters.hasAnyAttributes(EXTRACTOR_REALTIME_MODE_KEY, SOURCE_REALTIME_MODE_KEY)) {
      realtimeExtractor = new PipeRealtimeDataRegionHybridSource();
      LOGGER.info(
          "Pipe: '{}' ('{}') and '{}' ('{}') is not set, use hybrid mode by default.",
          EXTRACTOR_MODE_STREAMING_KEY,
          SOURCE_MODE_STREAMING_KEY,
          EXTRACTOR_REALTIME_MODE_KEY,
          SOURCE_REALTIME_MODE_KEY);
      return;
    }

    if (parameters.hasAnyAttributes(EXTRACTOR_MODE_STREAMING_KEY, SOURCE_MODE_STREAMING_KEY)) {
      final boolean isStreamingMode =
          parameters.getBooleanOrDefault(
              Arrays.asList(EXTRACTOR_MODE_STREAMING_KEY, SOURCE_MODE_STREAMING_KEY),
              EXTRACTOR_MODE_STREAMING_DEFAULT_VALUE);
      if (isStreamingMode) {
        realtimeExtractor = new PipeRealtimeDataRegionHybridSource();
      } else {
        realtimeExtractor = new PipeRealtimeDataRegionTsFileSource();
      }
      return;
    }

    switch (parameters.getStringByKeys(EXTRACTOR_REALTIME_MODE_KEY, SOURCE_REALTIME_MODE_KEY)) {
      case EXTRACTOR_REALTIME_MODE_FILE_VALUE:
      case EXTRACTOR_REALTIME_MODE_BATCH_MODE_VALUE:
        realtimeExtractor = new PipeRealtimeDataRegionTsFileSource();
        break;
      case EXTRACTOR_REALTIME_MODE_HYBRID_VALUE:
      case EXTRACTOR_REALTIME_MODE_LOG_VALUE:
      case EXTRACTOR_REALTIME_MODE_STREAM_MODE_VALUE:
        realtimeExtractor = new PipeRealtimeDataRegionHybridSource();
        break;
      case EXTRACTOR_REALTIME_MODE_FORCED_LOG_VALUE:
        realtimeExtractor = new PipeRealtimeDataRegionLogSource();
        break;
      default:
        realtimeExtractor = new PipeRealtimeDataRegionHybridSource();
        if (LOGGER.isWarnEnabled()) {
          LOGGER.warn(
              "Pipe: Unsupported source realtime mode: {}, create a hybrid source.",
              parameters.getStringByKeys(EXTRACTOR_REALTIME_MODE_KEY, SOURCE_REALTIME_MODE_KEY));
        }
    }
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    if (hasNoExtractionNeed) {
      return;
    }

    super.customize(parameters, configuration);

    historicalExtractor.customize(parameters, configuration);
    realtimeExtractor.customize(parameters, configuration);

    // Set watermark injector
    long watermarkIntervalInMs = EXTRACTOR_WATERMARK_INTERVAL_DEFAULT_VALUE;
    if (parameters.hasAnyAttributes(
        EXTRACTOR_WATERMARK_INTERVAL_KEY, SOURCE_WATERMARK_INTERVAL_KEY)) {
      watermarkIntervalInMs =
          parameters.getLongOrDefault(
              Arrays.asList(_EXTRACTOR_WATERMARK_INTERVAL_KEY, _SOURCE_WATERMARK_INTERVAL_KEY),
              EXTRACTOR_WATERMARK_INTERVAL_DEFAULT_VALUE);
    } else if (parameters.hasAnyAttributes(
        _EXTRACTOR_WATERMARK_INTERVAL_KEY, _SOURCE_WATERMARK_INTERVAL_KEY)) {
      watermarkIntervalInMs =
          parameters.getLongOrDefault(
              Arrays.asList(_EXTRACTOR_WATERMARK_INTERVAL_KEY, _SOURCE_WATERMARK_INTERVAL_KEY),
              EXTRACTOR_WATERMARK_INTERVAL_DEFAULT_VALUE);
    }
    if (watermarkIntervalInMs > 0) {
      watermarkInjector = new DataRegionWatermarkInjector(regionId, watermarkIntervalInMs);
      LOGGER.info(
          "Pipe {}@{}: Set watermark injector with interval {} ms.",
          pipeName,
          regionId,
          watermarkInjector.getInjectionIntervalInMs());
    }

    // register metric after generating taskID
    PipeDataRegionSourceMetrics.getInstance().register(this);
    PipeTsFileToTabletsMetrics.getInstance().register(this);
    PipeDataNodeSinglePipeMetrics.getInstance().register(this);
  }

  @Override
  public void start() throws Exception {
    if (hasNoExtractionNeed || hasBeenStarted.get()) {
      return;
    }

    final long startTime = System.currentTimeMillis();
    LOGGER.info(
        "Pipe {}@{}: Starting historical source {} and realtime source {}.",
        pipeName,
        regionId,
        historicalExtractor.getClass().getSimpleName(),
        realtimeExtractor.getClass().getSimpleName());

    super.start();

    final AtomicReference<Exception> exceptionHolder = new AtomicReference<>(null);
    final DataRegionId dataRegionIdObject = new DataRegionId(this.regionId);
    while (true) {
      // try to start sources in the data region ...
      // first try to run if data region exists, then try to run if data region does not exist.
      // both conditions fail is not common, which means the data region is created during the
      // runIfPresent and runIfAbsent operations. in this case, we need to retry.
      if (StorageEngine.getInstance()
              .runIfPresent(
                  dataRegionIdObject,
                  (dataRegion -> {
                    dataRegion.writeLock(
                        String.format("Pipe: starting %s", IoTDBDataRegionSource.class.getName()));
                    try {
                      startHistoricalExtractorAndRealtimeExtractor(exceptionHolder);
                    } finally {
                      dataRegion.writeUnlock();
                    }
                  }))
          || StorageEngine.getInstance()
              .runIfAbsent(
                  dataRegionIdObject,
                  () -> startHistoricalExtractorAndRealtimeExtractor(exceptionHolder))) {
        rethrowExceptionIfAny(exceptionHolder);

        LOGGER.info(
            "Pipe {}@{}: Started historical source {} and realtime source {} successfully within {} ms.",
            pipeName,
            regionId,
            historicalExtractor.getClass().getSimpleName(),
            realtimeExtractor.getClass().getSimpleName(),
            System.currentTimeMillis() - startTime);
        return;
      }
      rethrowExceptionIfAny(exceptionHolder);
    }
  }

  private void startHistoricalExtractorAndRealtimeExtractor(
      final AtomicReference<Exception> exceptionHolder) {
    try {
      // Start realtimeExtractor first to avoid losing data. This may cause some
      // retransmission, yet it is OK according to the idempotency of IoTDB.
      // Note: The order of historical collection is flushing data -> adding all tsFile events.
      // There can still be writing when tsFile events are added. If we start
      // realtimeExtractor after the process, then this part of data will be lost.
      realtimeExtractor.start();
      historicalExtractor.start();
    } catch (final Exception e) {
      exceptionHolder.set(e);
      LOGGER.warn(
          "Pipe {}@{}: Start historical source {} and realtime source {} error.",
          pipeName,
          regionId,
          historicalExtractor.getClass().getSimpleName(),
          realtimeExtractor.getClass().getSimpleName(),
          e);
    }
  }

  private void rethrowExceptionIfAny(final AtomicReference<Exception> exceptionHolder) {
    if (exceptionHolder.get() != null) {
      throw new PipeException("failed to start sources.", exceptionHolder.get());
    }
  }

  @Override
  public Event supply() throws Exception {
    if (hasNoExtractionNeed) {
      return null;
    }

    Event event = null;
    if (!historicalExtractor.hasConsumedAll()) {
      event = historicalExtractor.supply();
    } else {
      if (Objects.nonNull(watermarkInjector)) {
        event = watermarkInjector.inject();
      }
      if (Objects.isNull(event)) {
        event = realtimeExtractor.supply();
      }
    }

    if (Objects.nonNull(event)) {
      if (event instanceof TabletInsertionEvent) {
        PipeDataRegionSourceMetrics.getInstance().markTabletEvent(taskID);
      } else if (event instanceof TsFileInsertionEvent) {
        PipeDataRegionSourceMetrics.getInstance().markTsFileEvent(taskID);
      } else if (event instanceof PipeHeartbeatEvent) {
        PipeDataRegionSourceMetrics.getInstance().markPipeHeartbeatEvent(taskID);
      }
    }

    return event;
  }

  @Override
  public void close() throws Exception {
    if (hasNoExtractionNeed || !hasBeenStarted.get()) {
      return;
    }

    historicalExtractor.close();
    realtimeExtractor.close();
    if (Objects.nonNull(taskID)) {
      PipeDataRegionSourceMetrics.getInstance().deregister(taskID);
    }
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public int getHistoricalTsFileInsertionEventCount() {
    return hasBeenStarted.get() && Objects.nonNull(historicalExtractor)
        ? historicalExtractor.getPendingQueueSize()
        : 0;
  }

  public int getTabletInsertionEventCount() {
    return hasBeenStarted.get() ? realtimeExtractor.getTabletInsertionEventCount() : 0;
  }

  public int getRealtimeTsFileInsertionEventCount() {
    return hasBeenStarted.get() ? realtimeExtractor.getTsFileInsertionEventCount() : 0;
  }

  public int getPipeHeartbeatEventCount() {
    return hasBeenStarted.get() ? realtimeExtractor.getPipeHeartbeatEventCount() : 0;
  }
}
