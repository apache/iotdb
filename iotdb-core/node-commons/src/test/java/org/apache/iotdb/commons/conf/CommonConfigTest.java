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

package org.apache.iotdb.commons.conf;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

public class CommonConfigTest {

  @Test
  public void testSubscriptionDisabledInCommonConfig() {
    assertFalse(CommonConfig.SUBSCRIPTION_ENABLED);
    assertFalse(new CommonConfig().getSubscriptionEnabled());
  }

  @Test
  public void testSubscriptionIsNotExposedInConfigurationTemplate() throws IOException {
    assertNull(ConfigurationFileUtils.getConfigurationDefaultValue("subscription_enabled"));
  }
}
