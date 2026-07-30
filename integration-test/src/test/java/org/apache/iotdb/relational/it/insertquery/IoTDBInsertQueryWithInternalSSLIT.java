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

package org.apache.iotdb.relational.it.insertquery;

import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.itbase.category.TableClusterIT;
import org.apache.iotdb.itbase.category.TableLocalStandaloneIT;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.experimental.categories.Category;

import java.io.File;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.Statement;

import static org.junit.Assert.fail;

@Ignore
@Category({TableLocalStandaloneIT.class, TableClusterIT.class})
public class IoTDBInsertQueryWithInternalSSLIT extends IoTDBInsertQueryIT {

  private final String keyDir =
      System.getProperty("user.dir")
          + File.separator
          + "target"
          + File.separator
          + "test-classes"
          + File.separator;

  @Before
  @Override
  public void setUp() throws SQLException {
    EnvFactory.getEnv().cleanClusterEnvironment();
    EnvFactory.getEnv()
        .getConfig()
        .getCommonConfig()
        .setPartitionInterval(1000)
        .setMemtableSizeThreshold(10000)
        .setEnableInternalSSL(true)
        .setKeyStorePath(keyDir + "test-keystore")
        .setKeyStorePwd("thrift")
        .setTrustStorePath(keyDir + "test-truststore")
        .setTrustStorePwd("thrift");
    CommonDescriptor.getInstance().getConfig().setEnableInternalSSL(true);
    CommonDescriptor.getInstance().getConfig().setKeyStorePath(keyDir + "test-keystore");
    CommonDescriptor.getInstance().getConfig().setKeyStorePwd("thrift");
    CommonDescriptor.getInstance().getConfig().setTrustStorePath(keyDir + "test-truststore");
    CommonDescriptor.getInstance().getConfig().setTrustStorePwd("thrift");
    EnvFactory.getEnv().initClusterEnvironment();
    prepareDatabase();
    prepareData();
  }

  @After
  public void tearDown() {
    try (Connection connection = EnvFactory.getEnv().getConnection(BaseEnv.TABLE_SQL_DIALECT);
        Statement statement = connection.createStatement()) {
      statement.execute("DROP DATABASE IF EXISTS test");
    } catch (Exception e) {
      fail(e.getMessage());
    }
    CommonDescriptor.getInstance().getConfig().setEnableInternalSSL(false);
    CommonDescriptor.getInstance().getConfig().setKeyStorePath("");
    CommonDescriptor.getInstance().getConfig().setKeyStorePwd("");
    CommonDescriptor.getInstance().getConfig().setTrustStorePath("");
    CommonDescriptor.getInstance().getConfig().setTrustStorePwd("");
  }
}
