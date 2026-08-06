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

import org.junit.After;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CommonDescriptorPasswordExpirationTest {

  @After
  public void tearDown() {
    EditionGate.clearIoTDBOverrideForTest();
  }

  @Test
  public void loadsPasswordExpirationDaysProperty() throws Exception {
    EditionGate.setIoTDBOverrideForTest(false);
    CommonConfig config = CommonDescriptor.getInstance().getConfig();
    long original = config.getPasswordExpirationDays();
    try {
      TrimProperties properties = new TrimProperties();
      properties.setProperty("password_expiration_days", "30");
      properties.setProperty("password_expiration_seconds", "60");

      CommonDescriptor.getInstance().loadCommonProps(properties);

      assertEquals(30L, config.getPasswordExpirationDays());
    } finally {
      config.setPasswordExpirationDays(original);
    }
  }
}
