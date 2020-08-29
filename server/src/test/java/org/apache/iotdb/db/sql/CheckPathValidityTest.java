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

package org.apache.iotdb.db.sql;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.junit.Test;

public class CheckPathValidityTest {

  @Test
  public void testCheckPathValidity() {
    assertTrue(IoTDBConfig.PATH_PATTERN.matcher("root.vehicle").matches());
    assertTrue(IoTDBConfig.PATH_PATTERN.matcher("root.123456").matches());
    assertTrue(IoTDBConfig.PATH_PATTERN.matcher("root._1234").matches());
    assertTrue(IoTDBConfig.PATH_PATTERN.matcher("root._vehicle").matches());
    assertTrue(IoTDBConfig.PATH_PATTERN.matcher("root.1234a4").matches());
    assertTrue(IoTDBConfig.PATH_PATTERN.matcher("root.1_2").matches());
    assertTrue(IoTDBConfig.PATH_PATTERN.matcher("root.vehicle.1245.1.2.3").matches());
    assertTrue(IoTDBConfig.PATH_PATTERN.matcher("root.vehicle.1245.\"1.2.3\"").matches());
    assertTrue(IoTDBConfig.PATH_PATTERN.matcher("root.vehicle.1245.\"1.2.3\"").matches());

    assertFalse(IoTDBConfig.PATH_PATTERN.matcher("vehicle").matches());
    assertFalse(IoTDBConfig.PATH_PATTERN.matcher("root.\tvehicle").matches());
    assertFalse(IoTDBConfig.PATH_PATTERN.matcher("root.\nvehicle").matches());
    assertFalse(IoTDBConfig.PATH_PATTERN.matcher("root..vehicle").matches());
    assertTrue(IoTDBConfig.PATH_PATTERN.matcher("root.%12345").matches());
    assertTrue(IoTDBConfig.PATH_PATTERN.matcher("root.a{12345}").matches());
    assertTrue(IoTDBConfig.PATH_PATTERN.matcher("root.a[12345]").matches());
    assertTrue(IoTDBConfig.PATH_PATTERN.matcher("root.2e3.2-2.-1.%$#/&@.a[12345]{}").matches());
    assertTrue(IoTDBConfig.PATH_PATTERN.matcher("root.a.b.\"c.a\"").matches());
    assertTrue(IoTDBConfig.PATH_PATTERN.matcher("root.\"a.d\".b.\"c.a\"").matches());
  }
}
