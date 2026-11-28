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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iotdb.commons.conf;

import org.junit.Test;

import java.io.IOException;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class CommonDescriptorRetryPropertiesTest {

  private static final String RETRY_KEY = "enable_retry_for_unknown_error";
  private static final String REMOTE_RETRY_KEY =
      "write_request_remote_dispatch_max_retry_duration_in_ms";

  @Test
  public void testInvalidBooleanFallsBackToAndRecordsTemplateDefault() throws IOException {
    final CommonDescriptor descriptor = CommonDescriptor.getInstance();
    final CommonConfig config = descriptor.getConfig();
    final boolean originalRetryValue = config.isRetryForUnknownErrors();
    final long originalRemoteRetryValue = config.getRemoteWriteMaxRetryDurationInMs();
    final Map<String, String> appliedProperties = ConfigurationFileUtils.getAppliedProperties();
    final String originalRetryAppliedValue = appliedProperties.get(RETRY_KEY);
    final String originalRemoteRetryAppliedValue = appliedProperties.get(REMOTE_RETRY_KEY);
    final TrimProperties properties = new TrimProperties();
    properties.setProperty(RETRY_KEY, "invalid-boolean");

    try {
      config.setRetryForUnknownErrors(true);
      appliedProperties.remove(RETRY_KEY);
      descriptor.loadRetryProperties(properties);

      assertFalse(config.isRetryForUnknownErrors());
      assertEquals("false", appliedProperties.get(RETRY_KEY));
    } finally {
      config.setRetryForUnknownErrors(originalRetryValue);
      config.setRemoteWriteMaxRetryDurationInMs(originalRemoteRetryValue);
      restoreAppliedValue(appliedProperties, RETRY_KEY, originalRetryAppliedValue);
      restoreAppliedValue(appliedProperties, REMOTE_RETRY_KEY, originalRemoteRetryAppliedValue);
    }
  }

  private static void restoreAppliedValue(
      final Map<String, String> appliedProperties, final String key, final String value) {
    if (value == null) {
      appliedProperties.remove(key);
    } else {
      appliedProperties.put(key, value);
    }
  }
}
