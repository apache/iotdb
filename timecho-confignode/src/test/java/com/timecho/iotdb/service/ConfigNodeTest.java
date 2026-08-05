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

package com.timecho.iotdb.service;

import org.apache.iotdb.commons.exception.StartupException;

import com.timecho.iotdb.i18n.TimechoConfigNodeMessages;
import org.junit.Assert;
import org.junit.Test;

public class ConfigNodeTest {

  @Test
  public void testSecureEraseIsRejectedOnWindows() {
    final StartupException exception =
        Assert.assertThrows(
            StartupException.class, () -> ConfigNode.checkSecureEraseCompatibility(true, true));

    Assert.assertEquals(
        TimechoConfigNodeMessages
            .EXCEPTION_CONFIGNODE_CANNOT_START_ON_WINDOWS_WHEN_ENABLE_SECURE_ERASE_IS_TRUE_SET_ENABLE_SECURE_ERASE_TO_FALSE_AND_RESTART_CONFIGNODE_05C8378F,
        exception.getMessage());
  }

  @Test
  public void testSecureEraseCompatibilityForSupportedConfigurations() throws StartupException {
    ConfigNode.checkSecureEraseCompatibility(true, false);
    ConfigNode.checkSecureEraseCompatibility(false, true);
    ConfigNode.checkSecureEraseCompatibility(false, false);
  }
}
