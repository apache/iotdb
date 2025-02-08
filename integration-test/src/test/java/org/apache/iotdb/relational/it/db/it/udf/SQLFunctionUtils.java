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

package org.apache.iotdb.relational.it.db.it.udf;

import org.apache.iotdb.it.env.EnvFactory;

import org.junit.Assert;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_USER_DEFINED_AGG_FUNC;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_USER_DEFINED_SCALAR_FUNC;
import static org.apache.iotdb.commons.conf.IoTDBConstant.FUNCTION_TYPE_USER_DEFINED_TABLE_FUNC;
import static org.junit.Assert.fail;

public class SQLFunctionUtils {
  public static void createUDF(String udfName, String classPath) {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      // create
      statement.execute(String.format("create function %s as '%s'", udfName, classPath));
      // check
      try (ResultSet resultSet = statement.executeQuery("show functions")) {
        boolean found = false;
        while (resultSet.next()) {
          if (resultSet.getString(1).equals(udfName.toUpperCase())
              && resultSet.getString(3).equals(classPath)) {
            found = true;
            break;
          }
        }
        Assert.assertTrue("Can not find function", found);
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }

  public static void dropAllUDF() {
    try (Connection connection = EnvFactory.getEnv().getTableConnection();
        Statement statement = connection.createStatement()) {
      List<String> udfName = new ArrayList<>();
      try (ResultSet resultSet = statement.executeQuery("show functions")) {
        Set<String> externalUDF =
            new HashSet<>(
                Arrays.asList(
                    FUNCTION_TYPE_USER_DEFINED_SCALAR_FUNC,
                    FUNCTION_TYPE_USER_DEFINED_AGG_FUNC,
                    FUNCTION_TYPE_USER_DEFINED_TABLE_FUNC));
        while (resultSet.next()) {
          if (externalUDF.contains(resultSet.getString(2))) {
            udfName.add(resultSet.getString(1));
          }
        }
      }
      for (String name : udfName) {
        statement.execute(String.format("drop function %s", name));
      }
    } catch (Exception e) {
      fail(e.getMessage());
    }
  }
}
