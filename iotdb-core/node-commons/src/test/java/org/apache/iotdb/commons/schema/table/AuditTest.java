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

package org.apache.iotdb.commons.schema.table;

import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class AuditTest {

  @Test
  public void testIsAuditDatabase() {
    assertTrue(Audit.isAuditDatabase("__audit"));
    assertTrue(Audit.isAuditDatabase("root.__audit"));
    assertTrue(Audit.isAuditDatabase("__AUDIT"));
    assertTrue(Audit.isAuditDatabase("ROOT.__AUDIT"));
    assertTrue(Audit.isAuditDatabase("__audit.data"));
    assertTrue(Audit.isAuditDatabase("root.__audit.data"));
    assertFalse(Audit.isAuditDatabase("__audit_data"));
    assertFalse(Audit.isAuditDatabase("root.__audit_data"));
    assertFalse(Audit.isAuditDatabase(null));
  }
}
