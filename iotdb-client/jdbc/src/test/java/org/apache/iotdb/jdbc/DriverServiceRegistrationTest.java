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

package org.apache.iotdb.jdbc;

import org.junit.Test;

import java.sql.Driver;
import java.util.ServiceLoader;

import static org.junit.Assert.assertTrue;

public class DriverServiceRegistrationTest {

  /**
   * JDBC 4 drivers are loaded by DriverManager through ServiceLoader, from
   * META-INF/services/java.sql.Driver. Asking ServiceLoader directly keeps this independent of
   * whether another test has already loaded IoTDBDriver, which registers it with DriverManager as a
   * side effect.
   */
  @Test
  public void driverIsListedAsAJdbcServiceProvider() {
    boolean found = false;
    for (Driver driver : ServiceLoader.load(Driver.class)) {
      if (driver instanceof IoTDBDriver) {
        found = true;
      }
    }
    assertTrue("META-INF/services/java.sql.Driver should name IoTDBDriver", found);
  }
}
