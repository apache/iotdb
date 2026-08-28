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

package org.apache.iotdb.commons.conf;

import org.apache.iotdb.commons.audit.AuditEventType;

import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class CommonConfigAuditEventTest {

  @Test
  public void defaultControlEventsIncludeConnectionLimitRejection() {
    assertTrue(
        new CommonConfig()
            .getAuditableControlEventType()
            .contains(AuditEventType.LOGIN_RESOURCE_RESTRICT));
  }

  @Test
  public void defaultControlEventsIncludeTrustedChannelFailure() {
    assertTrue(
        new CommonConfig()
            .getAuditableControlEventType()
            .contains(AuditEventType.TRUSTED_CHANNEL_FUNCTION_FAILURE));
  }
}
