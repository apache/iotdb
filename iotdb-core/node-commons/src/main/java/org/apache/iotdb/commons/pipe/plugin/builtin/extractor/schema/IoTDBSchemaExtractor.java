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

package org.apache.iotdb.commons.pipe.plugin.builtin.extractor.schema;

import org.apache.iotdb.commons.pipe.metric.PipeFakeEventCounter;
import org.apache.iotdb.commons.pipe.task.connection.UnboundedBlockingPendingQueue;
import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.customizer.configuration.PipeExtractorRuntimeConfiguration;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;
import org.apache.iotdb.pipe.api.event.Event;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_AUTHORITY_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_DATA_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_FUNCTION_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_MODEL_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_SCHEMA_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_TRIGGER_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_TTL_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_INCLUSION_KEY;

public class IoTDBSchemaExtractor implements PipeExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBSchemaExtractor.class);

  private final AtomicBoolean hasBeenStarted;
  protected final UnboundedBlockingPendingQueue<Event> pendingQueue =
      new UnboundedBlockingPendingQueue<>(new PipeFakeEventCounter());

  private boolean enableSchemaSync = false;
  private boolean enableTtlSync = false;
  private boolean enableFunctionSync = false;
  private boolean enableTriggerSync = false;
  private boolean enableModelSync = false;
  private boolean enableAuthoritySync = false;
  private boolean atLeastOneEnable = false;

  public IoTDBSchemaExtractor() {
    this.hasBeenStarted = new AtomicBoolean(false);
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    validator.validate(
        arg -> {
          Set<String> inclusionList =
              new HashSet<>(Arrays.asList(((String) arg).replace(" ", "").split(",")));
          if (inclusionList.contains(EXTRACTOR_INCLUSION_SCHEMA_VALUE)) {
            enableSchemaSync = true;
          }
          if (inclusionList.contains(EXTRACTOR_INCLUSION_TTL_VALUE)) {
            enableTtlSync = true;
          }
          if (inclusionList.contains(EXTRACTOR_INCLUSION_FUNCTION_VALUE)) {
            enableFunctionSync = true;
          }
          if (inclusionList.contains(EXTRACTOR_INCLUSION_TRIGGER_VALUE)) {
            enableTriggerSync = true;
          }
          if (inclusionList.contains(EXTRACTOR_INCLUSION_MODEL_VALUE)) {
            enableModelSync = true;
          }
          if (inclusionList.contains(EXTRACTOR_INCLUSION_AUTHORITY_VALUE)) {
            enableAuthoritySync = true;
          }
          atLeastOneEnable =
              enableSchemaSync
                  || enableTtlSync
                  || enableFunctionSync
                  || enableTriggerSync
                  || enableModelSync
                  || enableAuthoritySync;
          // If none of above are present and data is also absent, then validation will fail.
          return atLeastOneEnable || inclusionList.contains(EXTRACTOR_INCLUSION_DATA_VALUE);
        },
        String.format(
            "At least one of %s, %s, %s, %s, %s, %s, %s should be present in %s.",
            EXTRACTOR_INCLUSION_DATA_VALUE,
            EXTRACTOR_INCLUSION_SCHEMA_VALUE,
            EXTRACTOR_INCLUSION_TTL_VALUE,
            EXTRACTOR_INCLUSION_FUNCTION_VALUE,
            EXTRACTOR_INCLUSION_TRIGGER_VALUE,
            EXTRACTOR_INCLUSION_MODEL_VALUE,
            EXTRACTOR_INCLUSION_AUTHORITY_VALUE,
            SOURCE_INCLUSION_KEY),
        validator
            .getParameters()
            .getStringOrDefault(
                Arrays.asList(EXTRACTOR_INCLUSION_KEY, SOURCE_INCLUSION_KEY),
                EXTRACTOR_INCLUSION_DEFAULT_VALUE));
  }

  @Override
  public void customize(PipeParameters parameters, PipeExtractorRuntimeConfiguration configuration)
      throws Exception {
    // do nothing
  }

  @Override
  public void start() throws Exception {
    if (hasBeenStarted.get()) {
      return;
    }
    hasBeenStarted.set(true);
    // TODO: start listen and assign
  }

  public final void extract(Event event) {
    // waitedOffer() should not return false, for the queue is unbounded.
    pendingQueue.waitedOffer(event);
  }

  @Override
  public Event supply() throws Exception {
    // TODO: manage reference count
    return pendingQueue.directPoll();
  }

  @Override
  public void close() throws Exception {
    // TODO: clear the queue and clear reference count
  }
}
