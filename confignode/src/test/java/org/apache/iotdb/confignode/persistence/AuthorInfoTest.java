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

package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.commons.auth.AuthException;
import org.apache.iotdb.commons.auth.authorizer.AuthorizerManager;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeConf;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;

import org.apache.commons.io.FileUtils;
import org.apache.thrift.TException;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;

import static org.apache.iotdb.db.constant.TestConstant.BASE_OUTPUT_PATH;

public class AuthorInfoTest {

  private static AuthorInfo authorInfo;
  private static final File snapshotDir = new File(BASE_OUTPUT_PATH, "authorInfo-snapshot");
  private static final ConfigNodeConf config = ConfigNodeDescriptor.getInstance().getConf();
  private static final CommonConfig commonConfig = CommonConfig.getInstance();

  @BeforeClass
  public static void setup() {
    authorInfo = AuthorInfo.getInstance();
    if (!snapshotDir.exists()) {
      snapshotDir.mkdirs();
    }
  }

  @AfterClass
  public static void cleanup() throws IOException, AuthException {
    authorInfo.clear();
    if (snapshotDir.exists()) {
      FileUtils.deleteDirectory(snapshotDir);
    }
  }

  @Test
  public void takeSnapshot() throws TException, IOException, AuthException {
    AuthorizerManager.getInstance().createRole("testRole");
    AuthorizerManager.getInstance().createUser("testUser", "testPassword");
    Assert.assertEquals(1, AuthorizerManager.getInstance().listAllRoles().size());
    Assert.assertEquals(2, AuthorizerManager.getInstance().listAllUsers().size());
    Assert.assertTrue(authorInfo.processTakeSnapshot(snapshotDir));
    authorInfo.clear();
    authorInfo.processLoadSnapshot(snapshotDir);
    Assert.assertEquals(1, AuthorizerManager.getInstance().listAllRoles().size());
    Assert.assertEquals(2, AuthorizerManager.getInstance().listAllUsers().size());
  }
}
