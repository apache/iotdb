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

package org.apache.iotdb.db.pipe.receiver.protocol.thrift;

import org.apache.iotdb.commons.audit.IAuditEntity;
import org.apache.iotdb.db.auth.AuthorityChecker;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

import java.util.Optional;

import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;

@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "javax.management.*"})
@RunWith(PowerMockRunner.class)
@PrepareForTest(AuthorityChecker.class)
public class IoTDBDataNodeReceiverAuditEntityTest {

  @Test
  public void testAuditEntityTakesTheUserIdFromTheUserManager() {
    mockStatic(AuthorityChecker.class);
    when(AuthorityChecker.getUserId("pipe_user")).thenReturn(Optional.of(10001L));

    final IAuditEntity auditEntity =
        new IoTDBDataNodeReceiver().createAuditEntity("pipe_user", "sender-host");

    Assert.assertEquals(10001L, auditEntity.getUserId());
    Assert.assertEquals("pipe_user", auditEntity.getUsername());
    Assert.assertEquals("sender-host", auditEntity.getCliHostname());
  }

  @Test
  public void testAuditEntityOfUnknownUserIsNotTheSuperUser() {
    mockStatic(AuthorityChecker.class);
    when(AuthorityChecker.getUserId("unknown_user")).thenReturn(Optional.empty());

    final IAuditEntity auditEntity =
        new IoTDBDataNodeReceiver().createAuditEntity("unknown_user", "sender-host");

    // The super user id would grant every table model privilege, so an unresolved user must never
    // be mapped to it.
    Assert.assertEquals(-1L, auditEntity.getUserId());
    Assert.assertEquals("unknown_user", auditEntity.getUsername());
  }

  @Test
  public void testAuditEntityWithoutUsernameIsNotTheSuperUser() {
    mockStatic(AuthorityChecker.class);

    final IAuditEntity auditEntity =
        new IoTDBDataNodeReceiver().createAuditEntity(null, "sender-host");

    Assert.assertEquals(-1L, auditEntity.getUserId());
    Assert.assertNull(auditEntity.getUsername());
  }
}
