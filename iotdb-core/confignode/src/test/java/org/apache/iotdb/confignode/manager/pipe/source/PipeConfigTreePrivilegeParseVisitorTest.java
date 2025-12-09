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

package org.apache.iotdb.confignode.manager.pipe.source;

import org.apache.iotdb.commons.auth.entity.PrivilegeUnion;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.confignode.manager.ConfigManager;
import org.apache.iotdb.confignode.manager.PermissionManager;
import org.apache.iotdb.confignode.rpc.thrift.TPermissionInfoResp;
import org.apache.iotdb.confignode.service.ConfigNode;
import org.apache.iotdb.db.exception.StorageEngineException;

import org.apache.tsfile.exception.write.WriteProcessException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;

public class PipeConfigTreePrivilegeParseVisitorTest {

  private final PipeConfigTreePrivilegeParseVisitor skipVisitor =
      new PipeConfigTreePrivilegeParseVisitor(true);
  private final PipeConfigTreePrivilegeParseVisitor throwVisitor =
      new PipeConfigTreePrivilegeParseVisitor(false);

  private ConfigNode oldInstance;

  @Before
  public void setUp()
      throws IOException, WriteProcessException, MetadataException, InterruptedException {
    oldInstance = ConfigNode.getInstance();
    ConfigNode.setInstance(new ConfigNode());
    final ConfigManager configManager = new ConfigManager();
    configManager.setPermissionManager(new TestPermissionManager());
    ConfigNode.getInstance().setConfigManager(configManager);
  }

  @After
  public void tearDown() throws IOException, StorageEngineException {
    ConfigNode.setInstance(oldInstance);
  }

  @Test
  public void testCreateDatabase() {
    Assert.assertTrue(skipVisitor.canReadSysSchema("root.db", null, true));
  }

  private static class TestPermissionManager extends PermissionManager {
    public TestPermissionManager() {
      super(null, null);
    }

    @Override
    public TPermissionInfoResp checkUserPrivileges(
        final String username, final PrivilegeUnion union) {
      return new TPermissionInfoResp(StatusUtils.OK);
    }
  }
}
