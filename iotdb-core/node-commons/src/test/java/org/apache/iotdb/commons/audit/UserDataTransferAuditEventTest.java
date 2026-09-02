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

package org.apache.iotdb.commons.audit;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;

import org.junit.Test;

import java.io.IOException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;

public class UserDataTransferAuditEventTest {

  @Test
  public void testRecordsOnlyMinimumTransferFields() {
    final UserDataTransferAuditEvent event =
        new UserDataTransferAuditEvent(
            new TEndPoint("127.0.0.1", 10740),
            new TEndPoint("127.0.0.2", 10740),
            new TEndPoint("127.0.0.1", 10740),
            UserDataTransferProtectionMethod.TLS,
            false,
            IOException.class.getName());

    assertEquals(IOException.class.getName(), event.getError());
    assertFalse(event.isSuccess());
  }
}
