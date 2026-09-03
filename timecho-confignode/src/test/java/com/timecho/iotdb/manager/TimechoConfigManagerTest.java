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

import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.i18n.UtilMessages;
import org.apache.iotdb.rpc.TSStatusCode;

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

  @Test
  public void schemaWritesAreRejectedWhenConfigNodeIsReadOnly() {
    TSStatus status = TimechoConfigManager.checkSchemaWritePermission(true, true);

    Assert.assertEquals(TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(), status.getCode());
  }

  @Test
  public void schemaWritesAreRejectedWhenConfigNodeIsNotActivated() {
    TSStatus status = TimechoConfigManager.checkSchemaWritePermission(false, false);

    Assert.assertEquals(TSStatusCode.LICENSE_ERROR.getStatusCode(), status.getCode());
    Assert.assertEquals(
        UtilMessages
            .MESSAGE_SCHEMA_WRITE_OPERATIONS_ARE_NOT_ALLOWED_UNTIL_THIS_NODE_IS_ACTIVATED_860A4054,
        status.getMessage());
  }

  @Test
  public void internalDatabaseCreationIsAllowedWhenConfigNodeIsNotActivated() {
    Assert.assertNull(TimechoConfigManager.checkSchemaWritePermission(false, false, true));
  }

  @Test
  public void internalDatabaseCreationIsStillRejectedWhenConfigNodeIsReadOnly() {
    TSStatus status = TimechoConfigManager.checkSchemaWritePermission(true, true, true);

    Assert.assertEquals(TSStatusCode.SYSTEM_READ_ONLY.getStatusCode(), status.getCode());
  }

  @Test
  public void unactivatedStatusTakesPrecedenceOverReadOnlyStatus() {
    TSStatus status = TimechoConfigManager.checkSchemaWritePermission(true, false);

    Assert.assertEquals(TSStatusCode.LICENSE_ERROR.getStatusCode(), status.getCode());
  }

  @Test
  public void schemaWritesAreAllowedWhenConfigNodeIsActivatedAndWritable() {
    Assert.assertNull(TimechoConfigManager.checkSchemaWritePermission(false, true));
  }
}
