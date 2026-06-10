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

package com.timecho.iotdb.manager;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TimechoConfigManagerTest {

  @Test
  public void singleV02LicenseShouldBeSharedByAllConfigNodes() {
    List<String> licenses = Collections.singletonList("02-ABCDE");

    Assert.assertTrue(TimechoConfigManager.useOneLicenseForAllConfigNodes(licenses));
    Assert.assertEquals("02-ABCDE", TimechoConfigManager.getLicenseForConfigNode(licenses, 0));
    Assert.assertEquals("02-ABCDE", TimechoConfigManager.getLicenseForConfigNode(licenses, 1));
    Assert.assertEquals("02-ABCDE", TimechoConfigManager.getLicenseForConfigNode(licenses, 2));
  }

  @Test
  public void singleV03LicenseShouldBeSharedByAllConfigNodes() {
    List<String> licenses = Collections.singletonList("03-ABCDE");

    Assert.assertTrue(TimechoConfigManager.useOneLicenseForAllConfigNodes(licenses));
    Assert.assertEquals("03-ABCDE", TimechoConfigManager.getLicenseForConfigNode(licenses, 0));
    Assert.assertEquals("03-ABCDE", TimechoConfigManager.getLicenseForConfigNode(licenses, 1));
    Assert.assertEquals("03-ABCDE", TimechoConfigManager.getLicenseForConfigNode(licenses, 2));
  }

  @Test
  public void legacyLicensesShouldStillBeMappedByConfigNodeIndex() {
    List<String> licenses = Arrays.asList("legacy-0", "legacy-1", "legacy-2");

    Assert.assertFalse(TimechoConfigManager.useOneLicenseForAllConfigNodes(licenses));
    Assert.assertEquals("legacy-0", TimechoConfigManager.getLicenseForConfigNode(licenses, 0));
    Assert.assertEquals("legacy-1", TimechoConfigManager.getLicenseForConfigNode(licenses, 1));
    Assert.assertEquals("legacy-2", TimechoConfigManager.getLicenseForConfigNode(licenses, 2));
  }

  @Test
  public void shortSingleLicenseShouldNotBeSharedByAllConfigNodes() {
    List<String> licenses = Collections.singletonList("02");

    Assert.assertFalse(TimechoConfigManager.useOneLicenseForAllConfigNodes(licenses));
  }
}
