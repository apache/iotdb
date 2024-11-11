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

package org.apache.iotdb.db.queryengine.plan.execution.config.sys.pipe;

import org.apache.iotdb.commons.pipe.config.constant.PipeExtractorConstant;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class PipeFunctionSupport {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeFunctionSupport.class);

  public static void applyNowFunctionToExtractorAttributes(
      final Map<String, String> extractorAttributes,
      final String sourceKey,
      final String extractorKey,
      final long currentTime) {
    final Pair<String, String> pair =
        getExtractorAttributesKeyAndValue(extractorAttributes, sourceKey, extractorKey);

    if (pair == null) {
      return;
    }
    if (isNowFunction(pair.right)) {
      extractorAttributes.replace(pair.left, String.valueOf(currentTime));
    }
  }

  private static Pair<String, String> getExtractorAttributesKeyAndValue(
      final Map<String, String> extractorAttributes,
      final String sourceKey,
      final String extractorKey) {
    String key = sourceKey;
    String value = extractorAttributes.get(key);
    if (value != null) {
      return new Pair<>(key, value);
    }

    // "source.".length() == 7
    try {
      key = sourceKey.substring(7);
      value = extractorAttributes.get(key);
    } catch (Exception e) {
      LOGGER.warn(
          "The prefix of sourceKey is not 'source.'. Please check the parameters passed in: {}",
          sourceKey,
          e);
    }
    if (value != null) {
      return new Pair<>(key, value);
    }

    key = extractorKey;
    value = extractorAttributes.get(key);
    if (value != null) {
      return new Pair<>(key, value);
    }
    return null;
  }

  private static boolean isNowFunction(final String value) {
    return PipeExtractorConstant.NOW_TIME_VALUE.equalsIgnoreCase(value.trim());
  }
}
