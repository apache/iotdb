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

package org.apache.iotdb.commons.pipe.source;

import org.apache.iotdb.commons.audit.AuditLogOperation;
import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskSourceRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TablePattern;
import org.apache.iotdb.commons.pipe.datastructure.pattern.TreePattern;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.annotation.TableModel;
import org.apache.iotdb.pipe.api.annotation.TreeModel;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;

import java.util.Arrays;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_IOTDB_SKIP_IF_NO_PRIVILEGES;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_IOTDB_USERNAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_IOTDB_USER_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_SKIP_IF_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_IOTDB_USERNAME_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_IOTDB_USER_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_SKIP_IF_KEY;
import static org.apache.iotdb.commons.pipe.datastructure.options.PipeInclusionOptions.getExclusionString;
import static org.apache.iotdb.commons.pipe.datastructure.options.PipeInclusionOptions.getInclusionString;
import static org.apache.iotdb.commons.pipe.datastructure.options.PipeInclusionOptions.hasAtLeastOneOption;
import static org.apache.iotdb.commons.pipe.datastructure.options.PipeInclusionOptions.optionsAreAllLegal;

@TreeModel
@TableModel
public abstract class IoTDBSource implements PipeExtractor {

  // Record these variables to provide corresponding value to tag key of monitoring metrics
  protected String taskID;
  protected String pipeName;
  protected long creationTime;
  protected int regionId;
  protected PipeTaskMeta pipeTaskMeta;

  protected boolean isForwardingPipeRequests;

  // The value is always true after the first start even the extractor is closed
  protected final AtomicBoolean hasBeenStarted = new AtomicBoolean(false);
  protected String userId;
  protected String userName;
  protected String cliHostname;
  protected UserEntity userEntity;
  protected boolean skipIfNoPrivileges = true;

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    final String inclusionString = getInclusionString(validator.getParameters());
    final String exclusionString = getExclusionString(validator.getParameters());
    final boolean isTreeDataListened =
        TreePattern.isTreeModelDataAllowToBeCaptured(validator.getParameters());
    final boolean isTableDataListened =
        TablePattern.isTableModelDataAllowToBeCaptured(validator.getParameters());

    validator
        .validate(
            args -> optionsAreAllLegal((String) args, isTreeDataListened, isTableDataListened),
            "The 'inclusion' string contains illegal path.",
            inclusionString)
        .validate(
            args -> optionsAreAllLegal((String) args, isTreeDataListened, isTableDataListened),
            "The 'inclusion.exclusion' string contains illegal path.",
            exclusionString)
        .validate(
            args ->
                hasAtLeastOneOption(
                    (String) args[0], (String) args[1], isTreeDataListened, isTableDataListened),
            "The pipe inclusion content can't be empty.",
            inclusionString,
            exclusionString);

    validator.validateSynonymAttributes(
        Arrays.asList(EXTRACTOR_IOTDB_USER_KEY, SOURCE_IOTDB_USER_KEY),
        Arrays.asList(EXTRACTOR_IOTDB_USERNAME_KEY, SOURCE_IOTDB_USERNAME_KEY),
        false);

    // Validate double living
    validateDoubleLiving(validator.getParameters());
  }

  private void validateDoubleLiving(final PipeParameters parameters) {
    final boolean isDoubleLiving =
        parameters.getBooleanOrDefault(
            Arrays.asList(
                PipeSourceConstant.EXTRACTOR_MODE_DOUBLE_LIVING_KEY,
                PipeSourceConstant.SOURCE_MODE_DOUBLE_LIVING_KEY),
            PipeSourceConstant.EXTRACTOR_MODE_DOUBLE_LIVING_DEFAULT_VALUE);
    if (!isDoubleLiving) {
      return;
    }

    // check 'capture.tree'
    final Boolean isCaptureTree =
        parameters.getBooleanByKeys(
            PipeSourceConstant.EXTRACTOR_CAPTURE_TREE_KEY,
            PipeSourceConstant.SOURCE_CAPTURE_TREE_KEY);
    if (Objects.nonNull(isCaptureTree) && !isCaptureTree) {
      throw new PipeParameterNotValidException(
          "capture.tree can not be specified to false when double living is enabled");
    }

    // check 'capture.table'
    final Boolean isCaptureTable =
        parameters.getBooleanByKeys(
            PipeSourceConstant.EXTRACTOR_CAPTURE_TABLE_KEY,
            PipeSourceConstant.SOURCE_CAPTURE_TABLE_KEY);
    if (Objects.nonNull(isCaptureTable) && !isCaptureTable) {
      throw new PipeParameterNotValidException(
          "capture.table can not be specified to false when double living is enabled");
    }

    // check 'forwarding-pipe-requests'
    final Boolean isForwardingPipeRequests =
        parameters.getBooleanByKeys(
            PipeSourceConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_KEY,
            PipeSourceConstant.SOURCE_FORWARDING_PIPE_REQUESTS_KEY);
    if (Objects.nonNull(isForwardingPipeRequests) && isForwardingPipeRequests) {
      throw new PipeParameterNotValidException(
          "forwarding-pipe-requests can not be specified to true when double living is enabled");
    }
  }

  @Override
  public void customize(
      final PipeParameters parameters, final PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    final PipeTaskSourceRuntimeEnvironment environment =
        ((PipeTaskSourceRuntimeEnvironment) configuration.getRuntimeEnvironment());
    regionId = environment.getRegionId();
    pipeName = environment.getPipeName();
    creationTime = environment.getCreationTime();
    taskID = pipeName + "_" + regionId + "_" + creationTime;
    pipeTaskMeta = environment.getPipeTaskMeta();

    final boolean isDoubleLiving =
        parameters.getBooleanOrDefault(
            Arrays.asList(
                PipeSourceConstant.EXTRACTOR_MODE_DOUBLE_LIVING_KEY,
                PipeSourceConstant.SOURCE_MODE_DOUBLE_LIVING_KEY),
            PipeSourceConstant.EXTRACTOR_MODE_DOUBLE_LIVING_DEFAULT_VALUE);
    if (isDoubleLiving) {
      isForwardingPipeRequests = false;
    } else {
      isForwardingPipeRequests =
          parameters.getBooleanOrDefault(
              Arrays.asList(
                  PipeSourceConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_KEY,
                  PipeSourceConstant.SOURCE_FORWARDING_PIPE_REQUESTS_KEY),
              PipeSourceConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_DEFAULT_VALUE);
    }

    userId =
        parameters.getStringOrDefault(
            Arrays.asList(
                PipeSourceConstant.EXTRACTOR_IOTDB_USER_ID,
                PipeSourceConstant.SOURCE_IOTDB_USER_ID),
            "-1");
    userName =
        parameters.getStringByKeys(
            PipeSourceConstant.EXTRACTOR_IOTDB_USER_KEY,
            PipeSourceConstant.SOURCE_IOTDB_USER_KEY,
            PipeSourceConstant.EXTRACTOR_IOTDB_USERNAME_KEY,
            PipeSourceConstant.SOURCE_IOTDB_USERNAME_KEY);
    cliHostname =
        parameters.getStringByKeys(
            PipeSourceConstant.EXTRACTOR_IOTDB_CLI_HOSTNAME,
            PipeSourceConstant.SOURCE_IOTDB_CLI_HOSTNAME);
    userEntity = new UserEntity(Long.parseLong(userId), userName, cliHostname);
    userEntity.setAuditLogOperation(AuditLogOperation.QUERY);

    skipIfNoPrivileges = getSkipIfNoPrivileges(parameters);
  }

  public static boolean getSkipIfNoPrivileges(final PipeParameters extractorParameters) {
    final String extractorSkipIfValue =
        extractorParameters
            .getStringOrDefault(
                Arrays.asList(EXTRACTOR_SKIP_IF_KEY, SOURCE_SKIP_IF_KEY),
                EXTRACTOR_IOTDB_SKIP_IF_NO_PRIVILEGES)
            .trim();
    final Set<String> skipIfOptionSet =
        Arrays.stream(extractorSkipIfValue.split(","))
            .map(String::trim)
            .filter(s -> !s.isEmpty())
            .map(String::toLowerCase)
            .collect(Collectors.toSet());
    boolean skipIfNoPrivileges = skipIfOptionSet.remove(EXTRACTOR_IOTDB_SKIP_IF_NO_PRIVILEGES);
    if (!skipIfOptionSet.isEmpty()) {
      throw new PipeParameterNotValidException(
          String.format("Parameters in set %s are not allowed in 'skipif'", skipIfOptionSet));
    }
    return skipIfNoPrivileges;
  }

  @Override
  public void start() throws Exception {
    if (hasBeenStarted.get()) {
      return;
    }
    hasBeenStarted.set(true);
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public String getTaskID() {
    return taskID;
  }

  public String getPipeName() {
    return pipeName;
  }

  public int getRegionId() {
    return regionId;
  }

  public long getCreationTime() {
    return creationTime;
  }
}
