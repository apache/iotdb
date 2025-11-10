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

package org.apache.iotdb.commons.pipe.sink.protocol;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskSinkRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.receiver.PipeReceiverStatusHandler;
import org.apache.iotdb.commons.pipe.sink.compressor.PipeCompressor;
import org.apache.iotdb.commons.pipe.sink.compressor.PipeCompressorConfig;
import org.apache.iotdb.commons.pipe.sink.compressor.PipeCompressorFactory;
import org.apache.iotdb.commons.pipe.sink.limiter.GlobalRPCRateLimiter;
import org.apache.iotdb.commons.pipe.sink.limiter.PipeEndPointRateLimiter;
import org.apache.iotdb.commons.pipe.sink.payload.thrift.request.PipeTransferCompressedReq;
import org.apache.iotdb.commons.utils.NodeUrlUtils;
import org.apache.iotdb.metrics.type.Histogram;
import org.apache.iotdb.metrics.type.Timer;
import org.apache.iotdb.pipe.api.PipeConnector;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeConnectorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeRuntimeEnvironment;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;
import org.apache.iotdb.service.rpc.thrift.TPipeTransferReq;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_COMPRESSOR_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_COMPRESSOR_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_COMPRESSOR_SET;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_COMPRESSOR_ZSTD_LEVEL_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_COMPRESSOR_ZSTD_LEVEL_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_COMPRESSOR_ZSTD_LEVEL_MAX_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_COMPRESSOR_ZSTD_LEVEL_MIN_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_CONFLICT_RECORD_IGNORED_DATA_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_CONFLICT_RECORD_IGNORED_DATA_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_CONFLICT_RESOLVE_STRATEGY_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_CONFLICT_RESOLVE_STRATEGY_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_CONFLICT_RETRY_MAX_TIME_SECONDS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_CONFLICT_RETRY_MAX_TIME_SECONDS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_DATA_CONVERT_ON_TYPE_MISMATCH_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_DATA_CONVERT_ON_TYPE_MISMATCH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_OTHERS_RECORD_IGNORED_DATA_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_OTHERS_RECORD_IGNORED_DATA_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_OTHERS_RETRY_MAX_TIME_SECONDS_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_EXCEPTION_OTHERS_RETRY_MAX_TIME_SECONDS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_FORMAT_HYBRID_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_FORMAT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_FORMAT_TABLET_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_FORMAT_TS_FILE_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_DELAY_MS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_DELAY_SECONDS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_MODE_ENABLE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_MODE_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_BATCH_SIZE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_CLI_HOSTNAME;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_HOST_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_IP_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_NODE_URLS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_PASSWORD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_PLAIN_BATCH_SIZE_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_PORT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_SKIP_IF_NO_PRIVILEGES;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_USERNAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_USER_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_USER_ID;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_IOTDB_USER_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LOAD_BALANCE_ROUND_ROBIN_STRATEGY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LOAD_BALANCE_STRATEGY_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LOAD_BALANCE_STRATEGY_SET;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LOAD_TSFILE_STRATEGY_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LOAD_TSFILE_STRATEGY_SET;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LOAD_TSFILE_STRATEGY_SYNC_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LOAD_TSFILE_VALIDATION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_LOAD_TSFILE_VALIDATION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_MARK_AS_GENERAL_WRITE_REQUEST_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_MARK_AS_GENERAL_WRITE_REQUEST_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_MARK_AS_PIPE_REQUEST_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_MARK_AS_PIPE_REQUEST_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_RATE_LIMIT_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_RATE_LIMIT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.CONNECTOR_SKIP_IF_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.IOTDB_CONNECTOR_SKIP_IF_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_COMPRESSOR_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_COMPRESSOR_ZSTD_LEVEL_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_EXCEPTION_CONFLICT_RECORD_IGNORED_DATA_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_EXCEPTION_CONFLICT_RESOLVE_STRATEGY_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_EXCEPTION_CONFLICT_RETRY_MAX_TIME_SECONDS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_EXCEPTION_DATA_CONVERT_ON_TYPE_MISMATCH_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_EXCEPTION_OTHERS_RECORD_IGNORED_DATA_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_EXCEPTION_OTHERS_RETRY_MAX_TIME_SECONDS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_FORMAT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_BATCH_DELAY_MS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_BATCH_DELAY_SECONDS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_BATCH_MODE_ENABLE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_BATCH_SIZE_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_CLI_HOSTNAME;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_HOST_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_IP_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_NODE_URLS_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_PASSWORD_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_PORT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_USERNAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_USER_ID;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_IOTDB_USER_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_LOAD_BALANCE_STRATEGY_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_LOAD_TSFILE_STRATEGY_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_LOAD_TSFILE_VALIDATION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_MARK_AS_GENERAL_WRITE_REQUEST_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_MARK_AS_PIPE_REQUEST_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_RATE_LIMIT_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant.SINK_SKIP_IF_KEY;

@TreeModel
@TableModel
public abstract class IoTDBSink implements PipeConnector {

  private static final String PARSE_URL_ERROR_FORMATTER =
      "Exception occurred while parsing node urls from target servers: {}";
  private static final String PARSE_URL_ERROR_MESSAGE =
      "Error occurred while parsing node urls from target servers, please check the specified 'host':'port' or 'node-urls'";

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSink.class);

  protected final List<TEndPoint> nodeUrls = new ArrayList<>();

  protected String userId = "-1";
  protected String username = CONNECTOR_IOTDB_USER_DEFAULT_VALUE;
  protected String cliHostname = "";
  protected UserEntity userEntity;
  protected String password = CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE;

  protected String loadBalanceStrategy;

  protected String loadTsFileStrategy;
  protected boolean loadTsFileValidation;

  protected boolean shouldMarkAsPipeRequest;
  protected boolean skipIfNoPrivileges;

  private boolean isRpcCompressionEnabled;
  private final List<PipeCompressor> compressors = new ArrayList<>();

  private static final Map<Pair<String, Long>, PipeEndPointRateLimiter>
      PIPE_END_POINT_RATE_LIMITER_MAP = new ConcurrentHashMap<>();
  private double endPointRateLimitBytesPerSecond = -1;
  private static final GlobalRPCRateLimiter GLOBAL_RATE_LIMITER = new GlobalRPCRateLimiter();

  protected boolean isTabletBatchModeEnabled = true;

  protected PipeReceiverStatusHandler receiverStatusHandler;
  protected boolean shouldReceiverConvertOnTypeMismatch =
      CONNECTOR_EXCEPTION_DATA_CONVERT_ON_TYPE_MISMATCH_DEFAULT_VALUE;

  private final AtomicLong totalUncompressedSize = new AtomicLong(0);
  private final AtomicLong totalCompressedSize = new AtomicLong(0);
  protected String attributeSortedString;
  protected Timer compressionTimer;
  protected boolean isRealtimeFirst;

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    final PipeParameters parameters = validator.getParameters();

    validator.validate(
        args ->
            (boolean) args[0]
                || (((boolean) args[1] || (boolean) args[2]) && (boolean) args[3])
                || (boolean) args[4]
                || (((boolean) args[5] || (boolean) args[6]) && (boolean) args[7]),
        String.format(
            "One of %s, %s:%s, %s, %s:%s must be specified",
            CONNECTOR_IOTDB_NODE_URLS_KEY,
            CONNECTOR_IOTDB_HOST_KEY,
            CONNECTOR_IOTDB_PORT_KEY,
            SINK_IOTDB_NODE_URLS_KEY,
            SINK_IOTDB_HOST_KEY,
            SINK_IOTDB_PORT_KEY),
        parameters.hasAttribute(CONNECTOR_IOTDB_NODE_URLS_KEY),
        parameters.hasAttribute(CONNECTOR_IOTDB_IP_KEY),
        parameters.hasAttribute(CONNECTOR_IOTDB_HOST_KEY),
        parameters.hasAttribute(CONNECTOR_IOTDB_PORT_KEY),
        parameters.hasAttribute(SINK_IOTDB_NODE_URLS_KEY),
        parameters.hasAttribute(SINK_IOTDB_IP_KEY),
        parameters.hasAttribute(SINK_IOTDB_HOST_KEY),
        parameters.hasAttribute(SINK_IOTDB_PORT_KEY));

    validator.validate(
        requestMaxBatchSizeInBytes -> (long) requestMaxBatchSizeInBytes > 0,
        String.format(
            "%s must be > 0, but got %s",
            SINK_IOTDB_BATCH_SIZE_KEY,
            parameters.getLongOrDefault(
                Arrays.asList(CONNECTOR_IOTDB_BATCH_SIZE_KEY, SINK_IOTDB_BATCH_SIZE_KEY),
                CONNECTOR_IOTDB_PLAIN_BATCH_SIZE_DEFAULT_VALUE)),
        parameters.getLongOrDefault(
            Arrays.asList(CONNECTOR_IOTDB_BATCH_SIZE_KEY, SINK_IOTDB_BATCH_SIZE_KEY),
            CONNECTOR_IOTDB_PLAIN_BATCH_SIZE_DEFAULT_VALUE));

    // Check coexistence of user and username
    validator.validateSynonymAttributes(
        Arrays.asList(CONNECTOR_IOTDB_USER_KEY, SINK_IOTDB_USER_KEY),
        Arrays.asList(CONNECTOR_IOTDB_USERNAME_KEY, SINK_IOTDB_USERNAME_KEY),
        false);

    // Check coexistence of batch.max-delay-ms and batch.max-delay-seconds
    validator.validateSynonymAttributes(
        Arrays.asList(CONNECTOR_IOTDB_BATCH_DELAY_MS_KEY, SINK_IOTDB_BATCH_DELAY_MS_KEY),
        Arrays.asList(CONNECTOR_IOTDB_BATCH_DELAY_SECONDS_KEY, SINK_IOTDB_BATCH_DELAY_SECONDS_KEY),
        false);

    // Check coexistence of mark-as-pipe-request and mark-as-general-write-request
    validator.validateSynonymAttributes(
        Arrays.asList(CONNECTOR_MARK_AS_PIPE_REQUEST_KEY, SINK_MARK_AS_PIPE_REQUEST_KEY),
        Arrays.asList(
            CONNECTOR_MARK_AS_GENERAL_WRITE_REQUEST_KEY, SINK_MARK_AS_GENERAL_WRITE_REQUEST_KEY),
        false);

    userId =
        parameters.getStringOrDefault(
            Arrays.asList(CONNECTOR_IOTDB_USER_ID, SINK_IOTDB_USER_ID), "-1");
    username =
        parameters.getStringOrDefault(
            Arrays.asList(
                CONNECTOR_IOTDB_USER_KEY,
                SINK_IOTDB_USER_KEY,
                CONNECTOR_IOTDB_USERNAME_KEY,
                SINK_IOTDB_USERNAME_KEY),
            CONNECTOR_IOTDB_USER_DEFAULT_VALUE);
    cliHostname =
        parameters.getStringOrDefault(
            Arrays.asList(CONNECTOR_IOTDB_CLI_HOSTNAME, SINK_IOTDB_CLI_HOSTNAME), "");
    userEntity = new UserEntity(Long.parseLong(userId), username, cliHostname);
    password =
        parameters.getStringOrDefault(
            Arrays.asList(CONNECTOR_IOTDB_PASSWORD_KEY, SINK_IOTDB_PASSWORD_KEY),
            CONNECTOR_IOTDB_PASSWORD_DEFAULT_VALUE);

    loadBalanceStrategy =
        parameters
            .getStringOrDefault(
                Arrays.asList(CONNECTOR_LOAD_BALANCE_STRATEGY_KEY, SINK_LOAD_BALANCE_STRATEGY_KEY),
                CONNECTOR_LOAD_BALANCE_ROUND_ROBIN_STRATEGY)
            .trim()
            .toLowerCase();
    validator.validate(
        arg -> CONNECTOR_LOAD_BALANCE_STRATEGY_SET.contains(loadBalanceStrategy),
        String.format(
            "Load balance strategy should be one of %s, but got %s.",
            CONNECTOR_LOAD_BALANCE_STRATEGY_SET, loadBalanceStrategy),
        loadBalanceStrategy);

    loadTsFileStrategy =
        parameters
            .getStringOrDefault(
                Arrays.asList(CONNECTOR_LOAD_TSFILE_STRATEGY_KEY, SINK_LOAD_TSFILE_STRATEGY_KEY),
                CONNECTOR_LOAD_TSFILE_STRATEGY_SYNC_VALUE)
            .trim()
            .toLowerCase();
    validator.validate(
        arg -> CONNECTOR_LOAD_TSFILE_STRATEGY_SET.contains(loadTsFileStrategy),
        String.format(
            "Load tsfile strategy should be one of %s, but got %s.",
            CONNECTOR_LOAD_TSFILE_STRATEGY_SET, loadTsFileStrategy),
        loadTsFileStrategy);
    loadTsFileValidation =
        parameters.getBooleanOrDefault(
            Arrays.asList(CONNECTOR_LOAD_TSFILE_VALIDATION_KEY, SINK_LOAD_TSFILE_VALIDATION_KEY),
            CONNECTOR_LOAD_TSFILE_VALIDATION_DEFAULT_VALUE);

    final int zstdCompressionLevel =
        parameters.getIntOrDefault(
            Arrays.asList(CONNECTOR_COMPRESSOR_ZSTD_LEVEL_KEY, SINK_COMPRESSOR_ZSTD_LEVEL_KEY),
            CONNECTOR_COMPRESSOR_ZSTD_LEVEL_DEFAULT_VALUE);
    validator.validate(
        arg ->
            (int) arg >= CONNECTOR_COMPRESSOR_ZSTD_LEVEL_MIN_VALUE
                && (int) arg <= CONNECTOR_COMPRESSOR_ZSTD_LEVEL_MAX_VALUE,
        String.format(
            "Zstd compression level should be in the range [%d, %d], but got %d.",
            CONNECTOR_COMPRESSOR_ZSTD_LEVEL_MIN_VALUE,
            CONNECTOR_COMPRESSOR_ZSTD_LEVEL_MAX_VALUE,
            zstdCompressionLevel),
        zstdCompressionLevel);

    final String compressionTypes =
        parameters
            .getStringOrDefault(
                Arrays.asList(CONNECTOR_COMPRESSOR_KEY, SINK_COMPRESSOR_KEY),
                CONNECTOR_COMPRESSOR_DEFAULT_VALUE)
            .toLowerCase();
    if (!compressionTypes.isEmpty()) {
      for (final String compressionType : compressionTypes.split(",")) {
        final String trimmedCompressionType = compressionType.trim();
        if (trimmedCompressionType.isEmpty()) {
          continue;
        }

        validator.validate(
            arg -> CONNECTOR_COMPRESSOR_SET.contains(trimmedCompressionType),
            String.format(
                "Compressor should be one of %s, but got %s.",
                CONNECTOR_COMPRESSOR_SET, trimmedCompressionType),
            trimmedCompressionType);
        compressors.add(
            PipeCompressorFactory.getCompressor(
                new PipeCompressorConfig(trimmedCompressionType, zstdCompressionLevel)));
      }
    }
    validator.validate(
        arg -> compressors.size() <= Byte.MAX_VALUE,
        String.format(
            "The number of compressors should be less than or equal to %d, but got %d.",
            Byte.MAX_VALUE, compressors.size()),
        compressors.size());
    isRpcCompressionEnabled = !compressors.isEmpty();

    endPointRateLimitBytesPerSecond =
        parameters.getDoubleOrDefault(
            Arrays.asList(CONNECTOR_RATE_LIMIT_KEY, SINK_RATE_LIMIT_KEY),
            CONNECTOR_RATE_LIMIT_DEFAULT_VALUE);
    validator.validate(
        arg -> endPointRateLimitBytesPerSecond <= Double.MAX_VALUE,
        String.format(
            "Rate limit should be in the range (0, %f], but got %f.",
            Double.MAX_VALUE, endPointRateLimitBytesPerSecond),
        endPointRateLimitBytesPerSecond);

    validator.validate(
        arg -> arg.equals("retry") || arg.equals("ignore"),
        String.format(
            "The value of key %s or %s must be either 'retry' or 'ignore'.",
            CONNECTOR_EXCEPTION_CONFLICT_RESOLVE_STRATEGY_KEY,
            SINK_EXCEPTION_CONFLICT_RESOLVE_STRATEGY_KEY),
        parameters
            .getStringOrDefault(
                Arrays.asList(
                    CONNECTOR_EXCEPTION_CONFLICT_RESOLVE_STRATEGY_KEY,
                    SINK_EXCEPTION_CONFLICT_RESOLVE_STRATEGY_KEY),
                CONNECTOR_EXCEPTION_CONFLICT_RESOLVE_STRATEGY_DEFAULT_VALUE)
            .trim()
            .toLowerCase());

    validator.validateAttributeValueRange(
        validator.getParameters().hasAttribute(CONNECTOR_FORMAT_KEY)
            ? CONNECTOR_FORMAT_KEY
            : SINK_FORMAT_KEY,
        true,
        CONNECTOR_FORMAT_TABLET_VALUE,
        CONNECTOR_FORMAT_HYBRID_VALUE,
        CONNECTOR_FORMAT_TS_FILE_VALUE);
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeConnectorRuntimeConfiguration configuration)
      throws Exception {
    final PipeRuntimeEnvironment environment = configuration.getRuntimeEnvironment();
    if (environment instanceof PipeTaskSinkRuntimeEnvironment) {
      attributeSortedString =
          ((PipeTaskSinkRuntimeEnvironment) environment).getAttributeSortedString();
    }

    nodeUrls.clear();
    nodeUrls.addAll(parseNodeUrls(parameters));
    LOGGER.info("IoTDBConnector nodeUrls: {}", nodeUrls);

    isTabletBatchModeEnabled =
        parameters.getBooleanOrDefault(
                Arrays.asList(
                    CONNECTOR_IOTDB_BATCH_MODE_ENABLE_KEY, SINK_IOTDB_BATCH_MODE_ENABLE_KEY),
                CONNECTOR_IOTDB_BATCH_MODE_ENABLE_DEFAULT_VALUE)
            || parameters
                .getStringOrDefault(
                    Arrays.asList(CONNECTOR_FORMAT_KEY, SINK_FORMAT_KEY),
                    CONNECTOR_FORMAT_HYBRID_VALUE)
                .equals(CONNECTOR_FORMAT_TS_FILE_VALUE);
    LOGGER.info("IoTDBConnector isTabletBatchModeEnabled: {}", isTabletBatchModeEnabled);

    final boolean shouldMarkAsGeneralWriteRequest =
        parameters.getBooleanOrDefault(
            Arrays.asList(
                CONNECTOR_MARK_AS_GENERAL_WRITE_REQUEST_KEY,
                SINK_MARK_AS_GENERAL_WRITE_REQUEST_KEY),
            CONNECTOR_MARK_AS_GENERAL_WRITE_REQUEST_DEFAULT_VALUE);
    if (shouldMarkAsGeneralWriteRequest) {
      shouldMarkAsPipeRequest = false;
    } else {
      shouldMarkAsPipeRequest =
          parameters.getBooleanOrDefault(
              Arrays.asList(CONNECTOR_MARK_AS_PIPE_REQUEST_KEY, SINK_MARK_AS_PIPE_REQUEST_KEY),
              CONNECTOR_MARK_AS_PIPE_REQUEST_DEFAULT_VALUE);
    }
    LOGGER.info("IoTDBConnector shouldMarkAsPipeRequest: {}", shouldMarkAsPipeRequest);

    final String connectorSkipIfValue =
        parameters
            .getStringOrDefault(
                Arrays.asList(CONNECTOR_SKIP_IF_KEY, SINK_SKIP_IF_KEY),
                IOTDB_CONNECTOR_SKIP_IF_DEFAULT_VALUE)
            .trim();
    final Set<String> skipIfOptionSet =
        Arrays.stream(connectorSkipIfValue.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .map(String::toLowerCase)
            .collect(Collectors.toSet());
    skipIfNoPrivileges = skipIfOptionSet.remove(CONNECTOR_IOTDB_SKIP_IF_NO_PRIVILEGES);
    if (!skipIfOptionSet.isEmpty()) {
      throw new PipeParameterNotValidException(
          String.format("Parameters in set %s are not allowed in 'skipif'", skipIfOptionSet));
    }
    LOGGER.info("IoTDBConnector skipIfNoPrivileges: {}", skipIfNoPrivileges);

    receiverStatusHandler =
        new PipeReceiverStatusHandler(
            parameters
                .getStringOrDefault(
                    Arrays.asList(
                        CONNECTOR_EXCEPTION_CONFLICT_RESOLVE_STRATEGY_KEY,
                        SINK_EXCEPTION_CONFLICT_RESOLVE_STRATEGY_KEY),
                    CONNECTOR_EXCEPTION_CONFLICT_RESOLVE_STRATEGY_DEFAULT_VALUE)
                .trim()
                .equalsIgnoreCase("retry"),
            parameters.getLongOrDefault(
                Arrays.asList(
                    CONNECTOR_EXCEPTION_CONFLICT_RETRY_MAX_TIME_SECONDS_KEY,
                    SINK_EXCEPTION_CONFLICT_RETRY_MAX_TIME_SECONDS_KEY),
                CONNECTOR_EXCEPTION_CONFLICT_RETRY_MAX_TIME_SECONDS_DEFAULT_VALUE),
            parameters.getBooleanOrDefault(
                Arrays.asList(
                    CONNECTOR_EXCEPTION_CONFLICT_RECORD_IGNORED_DATA_KEY,
                    SINK_EXCEPTION_CONFLICT_RECORD_IGNORED_DATA_KEY),
                CONNECTOR_EXCEPTION_CONFLICT_RECORD_IGNORED_DATA_DEFAULT_VALUE),
            parameters.getLongOrDefault(
                Arrays.asList(
                    CONNECTOR_EXCEPTION_OTHERS_RETRY_MAX_TIME_SECONDS_KEY,
                    SINK_EXCEPTION_OTHERS_RETRY_MAX_TIME_SECONDS_KEY),
                CONNECTOR_EXCEPTION_OTHERS_RETRY_MAX_TIME_SECONDS_DEFAULT_VALUE),
            parameters.getBooleanOrDefault(
                Arrays.asList(
                    CONNECTOR_EXCEPTION_OTHERS_RECORD_IGNORED_DATA_KEY,
                    SINK_EXCEPTION_OTHERS_RECORD_IGNORED_DATA_KEY),
                CONNECTOR_EXCEPTION_OTHERS_RECORD_IGNORED_DATA_DEFAULT_VALUE),
            skipIfNoPrivileges);
    shouldReceiverConvertOnTypeMismatch =
        parameters.getBooleanOrDefault(
            Arrays.asList(
                CONNECTOR_EXCEPTION_DATA_CONVERT_ON_TYPE_MISMATCH_KEY,
                SINK_EXCEPTION_DATA_CONVERT_ON_TYPE_MISMATCH_KEY),
            CONNECTOR_EXCEPTION_DATA_CONVERT_ON_TYPE_MISMATCH_DEFAULT_VALUE);
    LOGGER.info(
        "IoTDBConnector {} = {}",
        CONNECTOR_EXCEPTION_DATA_CONVERT_ON_TYPE_MISMATCH_KEY,
        shouldReceiverConvertOnTypeMismatch);
    isRealtimeFirst =
        parameters.getBooleanOrDefault(
            Arrays.asList(
                PipeSinkConstant.CONNECTOR_REALTIME_FIRST_KEY,
                PipeSinkConstant.SINK_REALTIME_FIRST_KEY),
            PipeSinkConstant.CONNECTOR_REALTIME_FIRST_DEFAULT_VALUE);
    LOGGER.info(
        "IoTDBConnector {} = {}", PipeSinkConstant.CONNECTOR_REALTIME_FIRST_KEY, isRealtimeFirst);
  }

  protected LinkedHashSet<TEndPoint> parseNodeUrls(final PipeParameters parameters)
      throws PipeParameterNotValidException {
    final LinkedHashSet<TEndPoint> givenNodeUrls = new LinkedHashSet<>(nodeUrls);

    try {
      if (parameters.hasAttribute(CONNECTOR_IOTDB_IP_KEY)
          && parameters.hasAttribute(CONNECTOR_IOTDB_PORT_KEY)) {
        givenNodeUrls.add(
            new TEndPoint(
                parameters.getStringByKeys(CONNECTOR_IOTDB_IP_KEY),
                parameters.getIntByKeys(CONNECTOR_IOTDB_PORT_KEY)));
      }

      if (parameters.hasAttribute(SINK_IOTDB_IP_KEY)
          && parameters.hasAttribute(SINK_IOTDB_PORT_KEY)) {
        givenNodeUrls.add(
            new TEndPoint(
                parameters.getStringByKeys(SINK_IOTDB_IP_KEY),
                parameters.getIntByKeys(SINK_IOTDB_PORT_KEY)));
      }

      if (parameters.hasAttribute(CONNECTOR_IOTDB_HOST_KEY)
          && parameters.hasAttribute(CONNECTOR_IOTDB_PORT_KEY)) {
        givenNodeUrls.add(
            new TEndPoint(
                parameters.getStringByKeys(CONNECTOR_IOTDB_HOST_KEY),
                parameters.getIntByKeys(CONNECTOR_IOTDB_PORT_KEY)));
      }

      if (parameters.hasAttribute(SINK_IOTDB_HOST_KEY)
          && parameters.hasAttribute(SINK_IOTDB_PORT_KEY)) {
        givenNodeUrls.add(
            new TEndPoint(
                parameters.getStringByKeys(SINK_IOTDB_HOST_KEY),
                parameters.getIntByKeys(SINK_IOTDB_PORT_KEY)));
      }

      if (parameters.hasAttribute(CONNECTOR_IOTDB_NODE_URLS_KEY)) {
        givenNodeUrls.addAll(
            NodeUrlUtils.parseTEndPointUrls(
                Arrays.asList(
                    parameters
                        .getStringByKeys(CONNECTOR_IOTDB_NODE_URLS_KEY)
                        .replace(" ", "")
                        .split(","))));
      }

      if (parameters.hasAttribute(SINK_IOTDB_NODE_URLS_KEY)) {
        givenNodeUrls.addAll(
            NodeUrlUtils.parseTEndPointUrls(
                Arrays.asList(
                    parameters
                        .getStringByKeys(SINK_IOTDB_NODE_URLS_KEY)
                        .replace(" ", "")
                        .split(","))));
      }
    } catch (final Exception e) {
      LOGGER.warn(PARSE_URL_ERROR_FORMATTER, e.toString());
      throw new PipeParameterNotValidException(PARSE_URL_ERROR_MESSAGE);
    }

    checkNodeUrls(givenNodeUrls);

    return givenNodeUrls;
  }

  private void checkNodeUrls(final Set<TEndPoint> nodeUrls) throws PipeParameterNotValidException {
    for (final TEndPoint nodeUrl : nodeUrls) {
      if (Objects.isNull(nodeUrl.ip) || nodeUrl.ip.isEmpty()) {
        LOGGER.warn(PARSE_URL_ERROR_FORMATTER, "host cannot be empty");
        throw new PipeParameterNotValidException(PARSE_URL_ERROR_MESSAGE);
      }
      if (nodeUrl.port == 0) {
        LOGGER.warn(PARSE_URL_ERROR_FORMATTER, "port cannot be empty");
        throw new PipeParameterNotValidException(PARSE_URL_ERROR_MESSAGE);
      }
    }
  }

  @Override
  public void close() {
    // TODO: Not all the limiters should be closed here, but it's fine for now.
    PIPE_END_POINT_RATE_LIMITER_MAP.clear();
  }

  public TPipeTransferReq compressIfNeeded(TPipeTransferReq req) throws IOException {
    // Explanation for +3: version 1 byte, type 2 bytes
    totalUncompressedSize.addAndGet(req.body.array().length + 3);
    if (isRpcCompressionEnabled) {
      final long time = System.nanoTime();
      req = PipeTransferCompressedReq.toTPipeTransferReq(req, compressors);
      if (Objects.nonNull(compressionTimer)) {
        compressionTimer.updateNanos(System.nanoTime() - time);
      }
    }
    // Explanation for +3: version 1 byte, type 2 bytes
    totalCompressedSize.addAndGet(req.body.array().length + 3);
    return req;
  }

  protected byte[] compressIfNeeded(byte[] reqInBytes) throws IOException {
    totalUncompressedSize.addAndGet(reqInBytes.length);
    if (isRpcCompressionEnabled) {
      final long time = System.nanoTime();
      reqInBytes = PipeTransferCompressedReq.toTPipeTransferReqBytes(reqInBytes, compressors);
      if (Objects.nonNull(compressionTimer)) {
        compressionTimer.updateNanos(System.nanoTime() - time);
      }
    }
    totalCompressedSize.addAndGet(reqInBytes.length);
    return reqInBytes;
  }

  public long getTotalCompressedSize() {
    return totalCompressedSize.get();
  }

  public long getTotalUncompressedSize() {
    return totalUncompressedSize.get();
  }

  public void rateLimitIfNeeded(
      final String pipeName,
      final long creationTime,
      final TEndPoint endPoint,
      final long bytesLength) {
    if (pipeName != null && endPointRateLimitBytesPerSecond > 0) {
      PIPE_END_POINT_RATE_LIMITER_MAP
          .computeIfAbsent(
              new Pair<>(pipeName, creationTime),
              endpoint ->
                  new PipeEndPointRateLimiter(
                      pipeName, creationTime, endPointRateLimitBytesPerSecond))
          .acquire(endPoint, bytesLength);
    }

    GLOBAL_RATE_LIMITER.acquire(bytesLength);
  }

  /**
   * When a pipe is dropped, the connector maybe reused and will not be closed. We need to discard
   * its batched or queued events in the output pipe connector.
   */
  public synchronized void discardEventsOfPipe(final String pipeName, final int regionId) {
    // Do nothing by default
  }

  public PipeReceiverStatusHandler statusHandler() {
    return receiverStatusHandler;
  }

  public void setTabletBatchSizeHistogram(Histogram tabletBatchSizeHistogram) {
    // do nothing by default
  }

  public void setTsFileBatchSizeHistogram(Histogram tsFileBatchSizeHistogram) {
    // do nothing by default
  }

  public void setTabletBatchTimeIntervalHistogram(Histogram tabletBatchTimeIntervalHistogram) {
    // do nothing by default
  }

  public void setTsFileBatchTimeIntervalHistogram(Histogram tsFileBatchTimeIntervalHistogram) {
    // do nothing by default
  }

  public void setBatchEventSizeHistogram(Histogram tsFileBatchTimeIntervalHistogram) {
    // do nothing by default
  }
}
