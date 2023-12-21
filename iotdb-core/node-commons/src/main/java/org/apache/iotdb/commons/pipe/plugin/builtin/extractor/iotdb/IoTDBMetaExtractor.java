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

package org.apache.iotdb.commons.pipe.plugin.builtin.extractor.iotdb;

import org.apache.iotdb.pipe.api.PipeExtractor;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameterValidator;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_AUTHORITY_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_DATA_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_FUNCTION_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_DEFAULT_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_INCLUSION_KEY;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_MODEL_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_SCHEMA_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_TRIGGER_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.EXTRACTOR_TTL_VALUE;
import static org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant.SOURCE_INCLUSION_KEY;

public abstract class IoTDBMetaExtractor implements PipeExtractor {

  private static final Logger LOGGER = LoggerFactory.getLogger(IoTDBMetaExtractor.class);

  private final AtomicBoolean hasBeenStarted;

  private boolean enableSchemaSync = false;
  private boolean enableTtlSync = false;
  private boolean enableFunctionSync = false;
  private boolean enableTriggerSync = false;
  private boolean enableModelSync = false;
  private boolean enableAuthoritySync = false;
  private boolean atLeastOneEnable = false;

  protected IoTDBMetaExtractor() {
    this.hasBeenStarted = new AtomicBoolean(false);
  }

  @Override
  public void validate(PipeParameterValidator validator) throws Exception {
    validator.validate(
        arg -> {
          Set<String> inclusionSet =
              new HashSet<>(Arrays.asList(((String) arg).replace(" ", "").split(",")));
          if (inclusionSet.contains(EXTRACTOR_SCHEMA_VALUE)) {
            enableSchemaSync = true;
          }
          if (inclusionSet.contains(EXTRACTOR_TTL_VALUE)) {
            enableTtlSync = true;
          }
          if (inclusionSet.contains(EXTRACTOR_FUNCTION_VALUE)) {
            enableFunctionSync = true;
          }
          if (inclusionSet.contains(EXTRACTOR_TRIGGER_VALUE)) {
            enableTriggerSync = true;
          }
          if (inclusionSet.contains(EXTRACTOR_MODEL_VALUE)) {
            enableModelSync = true;
          }
          if (inclusionSet.contains(EXTRACTOR_AUTHORITY_VALUE)) {
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
          return atLeastOneEnable || inclusionSet.contains(EXTRACTOR_DATA_VALUE);
        },
        String.format(
            "At least one of %s, %s, %s, %s, %s, %s, %s should be present in %s.",
            EXTRACTOR_DATA_VALUE,
            EXTRACTOR_SCHEMA_VALUE,
            EXTRACTOR_TTL_VALUE,
            EXTRACTOR_FUNCTION_VALUE,
            EXTRACTOR_TRIGGER_VALUE,
            EXTRACTOR_MODEL_VALUE,
            EXTRACTOR_AUTHORITY_VALUE,
            SOURCE_INCLUSION_KEY),
        validator
            .getParameters()
            .getStringOrDefault(
                Arrays.asList(EXTRACTOR_INCLUSION_KEY, SOURCE_INCLUSION_KEY),
                EXTRACTOR_INCLUSION_DEFAULT_VALUE));
  }
}
