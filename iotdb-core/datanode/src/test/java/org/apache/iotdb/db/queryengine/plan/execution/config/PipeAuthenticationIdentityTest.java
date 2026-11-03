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

package org.apache.iotdb.db.queryengine.plan.execution.config;

import org.apache.iotdb.commons.audit.UserEntity;
import org.apache.iotdb.commons.pipe.config.constant.PipeSinkConstant;
import org.apache.iotdb.commons.pipe.config.constant.PipeSourceConstant;
import org.apache.iotdb.commons.pipe.config.constant.SystemConstant;
import org.apache.iotdb.db.auth.AuthorityChecker;
import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest(AuthorityChecker.class)
public class PipeAuthenticationIdentityTest {

  @Test
  public void testModifiedCredentialsReplaceInjectedSuperUserId() {
    mockStatic(AuthorityChecker.class);
    when(AuthorityChecker.getUserId("thulab")).thenReturn(Optional.of(10001L));

    final Map<String, String> sourceAttributes = new HashMap<>();
    TableConfigTaskVisitor.checkAndEnrichSourceUser(
        "pipe", sourceAttributes, new UserEntity(0L, "root", "host"), false);

    final Map<String, String> sourceChanges = new HashMap<>();
    sourceChanges.put("username", "thulab");
    sourceChanges.put("password", "password");
    TableConfigTaskVisitor.markSourceAuthenticationAsExplicitIfNecessary(sourceChanges);
    new PipeParameters(sourceAttributes)
        .addOrReplaceEquivalentAttributes(new PipeParameters(sourceChanges));

    final Map<String, String> sinkAttributes = new HashMap<>();
    sinkAttributes.put(PipeSinkConstant.SINK_KEY, "write-back-sink");
    TableConfigTaskVisitor.checkAndEnrichSinkUser(
        "pipe", sinkAttributes, new UserEntity(0L, "root", "host"), false);

    final Map<String, String> sinkChanges = new HashMap<>();
    sinkChanges.put(PipeSinkConstant.CONNECTOR_IOTDB_USER_KEY, "thulab");
    sinkChanges.put(PipeSinkConstant.CONNECTOR_IOTDB_PASSWORD_KEY, "password");
    TableConfigTaskVisitor.markSinkAuthenticationAsExplicitIfNecessary(sinkChanges);
    new PipeParameters(sinkAttributes)
        .addOrReplaceEquivalentAttributes(new PipeParameters(sinkChanges));

    assertEquals("10001", sourceAttributes.get(PipeSourceConstant.SOURCE_IOTDB_USER_ID));
    assertEquals(
        Boolean.FALSE.toString(),
        sourceAttributes.get(SystemConstant.SOURCE_AUTHENTICATION_INJECTED_KEY));
    assertEquals("10001", sinkAttributes.get(PipeSinkConstant.SINK_IOTDB_USER_ID));
    assertEquals(
        Boolean.FALSE.toString(),
        sinkAttributes.get(SystemConstant.SINK_AUTHENTICATION_INJECTED_KEY));
  }

  @Test
  public void testExplicitCredentialsUseTheirOwnUserIdOnCreateAndReplace() {
    mockStatic(AuthorityChecker.class);
    when(AuthorityChecker.getUserId("thulab")).thenReturn(Optional.of(10001L));

    for (final boolean isAlter : new boolean[] {false, true}) {
      final Map<String, String> sourceAttributes = new HashMap<>();
      sourceAttributes.put(PipeSourceConstant.EXTRACTOR_IOTDB_USER_KEY, "thulab");
      sourceAttributes.put(PipeSourceConstant.EXTRACTOR_IOTDB_PASSWORD_KEY, "password");
      TableConfigTaskVisitor.checkAndEnrichSourceUser(
          "pipe", sourceAttributes, new UserEntity(0L, "root", "host"), isAlter);

      final Map<String, String> sinkAttributes = new HashMap<>();
      sinkAttributes.put(PipeSinkConstant.SINK_KEY, "write-back-sink");
      sinkAttributes.put(PipeSinkConstant.SINK_IOTDB_USERNAME_KEY, "thulab");
      sinkAttributes.put(PipeSinkConstant.SINK_IOTDB_PASSWORD_KEY, "password");
      TableConfigTaskVisitor.checkAndEnrichSinkUser(
          "pipe", sinkAttributes, new UserEntity(0L, "root", "host"), isAlter);

      assertEquals("10001", sourceAttributes.get(PipeSourceConstant.SOURCE_IOTDB_USER_ID));
      assertEquals("10001", sinkAttributes.get(PipeSinkConstant.SINK_IOTDB_USER_ID));
    }
  }

  @Test
  public void testPasswordOnlyModificationPreservesExistingUserId() {
    final Map<String, String> sourceAttributes = new HashMap<>();
    sourceAttributes.put(PipeSourceConstant.SOURCE_IOTDB_USER_ID, "10001");
    final Map<String, String> sourceChanges = new HashMap<>();
    sourceChanges.put("password", "new-password");
    TableConfigTaskVisitor.markSourceAuthenticationAsExplicitIfNecessary(sourceChanges);
    new PipeParameters(sourceAttributes)
        .addOrReplaceEquivalentAttributes(new PipeParameters(sourceChanges));

    final Map<String, String> sinkAttributes = new HashMap<>();
    sinkAttributes.put(PipeSinkConstant.SINK_IOTDB_USER_ID, "10001");
    final Map<String, String> sinkChanges = new HashMap<>();
    sinkChanges.put("password", "new-password");
    TableConfigTaskVisitor.markSinkAuthenticationAsExplicitIfNecessary(sinkChanges);
    new PipeParameters(sinkAttributes)
        .addOrReplaceEquivalentAttributes(new PipeParameters(sinkChanges));

    assertEquals("10001", sourceAttributes.get(PipeSourceConstant.SOURCE_IOTDB_USER_ID));
    assertEquals("10001", sinkAttributes.get(PipeSinkConstant.SINK_IOTDB_USER_ID));
  }
}
