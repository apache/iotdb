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

import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.pipe.agent.task.meta.PipeTaskMeta;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.plugin.env.PipeTaskSourceRuntimeEnvironment;
import org.apache.iotdb.commons.pipe.event.EnrichedEvent;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.exception.PipeException;
import org.apache.iotdb.pipe.api.exception.PipeParameterNotValidException;

import java.util.Arrays;
import java.util.Locale;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_EXCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_INCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.EXTRACTOR_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_EXCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant.SOURCE_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.datastructure.options.PipeInclusionOptions.hasAtLeastOneOption;
import static org.apache.iotdb.commons.pipe.datastructure.options.PipeInclusionOptions.optionsAreAllLegal;

public abstract class IoTDBSource implements PipeExtractor {

  // Record these variables to provide corresponding value to tag key of monitoring metrics
  protected String taskID;
  protected String pipeName;
  protected long creationTime;
  protected int regionId;
  protected PipeTaskMeta pipeTaskMeta;

  protected boolean isForwardingPipeRequests;
  protected String userName;
  protected String cliHostname;
  protected UserEntity userEntity;
  protected boolean skipIfNoPrivileges = true;

  // The value is always true after the first start even the extractor is closed
  protected final AtomicBoolean hasBeenStarted = new AtomicBoolean(false);

  @Override
  public void validate(final PipeParameterValidator validator) throws Exception {
    validator
        .validate(
            args -> optionsAreAllLegal((String) args),
            "The 'inclusion' string contains illegal path.",
            validator
                .getParameters()
                .getStringOrDefault(
                    Arrays.asList(EXTRACTOR_INCLUSION_KEY, SOURCE_INCLUSION_KEY),
                    EXTRACTOR_INCLUSION_DEFAULT_VALUE))
        .validate(
            args -> optionsAreAllLegal((String) args),
            "The 'inclusion.exclusion' string contains illegal path.",
            validator
                .getParameters()
                .getStringOrDefault(
                    Arrays.asList(EXTRACTOR_EXCLUSION_KEY, SOURCE_EXCLUSION_KEY),
                    EXTRACTOR_EXCLUSION_DEFAULT_VALUE))
        .validate(
            args -> hasAtLeastOneOption((String) args[0], (String) args[1]),
            "The pipe inclusion content can't be empty.",
            validator
                .getParameters()
                .getStringOrDefault(
                    Arrays.asList(EXTRACTOR_INCLUSION_KEY, SOURCE_INCLUSION_KEY),
                    EXTRACTOR_INCLUSION_DEFAULT_VALUE),
            validator
                .getParameters()
                .getStringOrDefault(
                    Arrays.asList(EXTRACTOR_EXCLUSION_KEY, SOURCE_EXCLUSION_KEY),
                    EXTRACTOR_EXCLUSION_DEFAULT_VALUE));
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

    isForwardingPipeRequests =
        parameters.getBooleanOrDefault(
            Arrays.asList(
                PipeSourceConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_KEY,
                PipeSourceConstant.SOURCE_FORWARDING_PIPE_REQUESTS_KEY),
            PipeSourceConstant.EXTRACTOR_FORWARDING_PIPE_REQUESTS_DEFAULT_VALUE);

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
    skipIfNoPrivileges = getSkipIfNoPrivileges(parameters);

    final boolean authenticationInjected =
        parameters.getBooleanOrDefault(
            org.apache.iotdb.commons.pipe.config.constant.SystemConstant
                .SOURCE_AUTHENTICATION_INJECTED_KEY,
            false);
    final String password =
        parameters.getStringByKeys(
            PipeSourceConstant.EXTRACTOR_IOTDB_PASSWORD_KEY,
            PipeSourceConstant.SOURCE_IOTDB_PASSWORD_KEY);
    if (userName != null && !userName.isEmpty() && password == null && !authenticationInjected) {
      throw new PipeParameterNotValidException(
          "Source password must be set when source username is specified.");
    }
    if ((userName == null || userName.isEmpty()) && password != null) {
      throw new PipeParameterNotValidException(
          "Source username must be set when source password is specified.");
    }
    userEntity =
        userName == null || userName.isEmpty() ? null : new UserEntity(-1, userName, cliHostname);
    if (password != null && !authenticationInjected) {
      try {
        login(password);
      } catch (final PipeException e) {
        throw e;
      } catch (final Exception e) {
        throw new PipeException(
            String.format("Failed to authenticate source user %s for pipe %s.", userName, pipeName),
            e);
      }
    }
  }

  protected void login(final String password) throws Exception {
    throw new PipeException(
        String.format("Source authentication is not supported for pipe %s.", pipeName));
  }

  protected final void bindSourceAuthentication(final Object event) {
    if (event instanceof EnrichedEvent) {
      ((EnrichedEvent) event).bindSourceAuthentication(userEntity, skipIfNoPrivileges);
    }
  }

  public static boolean getSkipIfNoPrivileges(final PipeParameters parameters) {
    final String value =
        parameters
            .getStringOrDefault(
                Arrays.asList(
                    PipeSourceConstant.EXTRACTOR_SKIP_IF_KEY,
                    PipeSourceConstant.SOURCE_SKIP_IF_KEY),
                PipeSourceConstant.EXTRACTOR_IOTDB_SKIP_IF_NO_PRIVILEGES)
            .trim();
    final Set<String> options =
        Arrays.stream(value.split(","))
            .map(String::trim)
            .filter(option -> !option.isEmpty())
            .map(option -> option.toLowerCase(Locale.ENGLISH))
            .collect(Collectors.toSet());
    final boolean skip = options.remove(PipeSourceConstant.EXTRACTOR_IOTDB_SKIP_IF_NO_PRIVILEGES);
    if (!options.isEmpty()) {
      throw new PipeParameterNotValidException(
          String.format("Unsupported source skipif options: %s", options));
    }
    return skip;
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
