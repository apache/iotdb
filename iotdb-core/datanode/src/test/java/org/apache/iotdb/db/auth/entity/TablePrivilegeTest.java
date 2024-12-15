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
package org.apache.iotdb.db.auth.entity;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.auth.entity.TablePrivilege;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

public class TablePrivilegeTest {
  @Test
  public void testTablePrivilege_Init() {
    TablePrivilege tablePrivilege = new TablePrivilege("table");
    tablePrivilege.grantPrivilege(PrivilegeType.SELECT);
    tablePrivilege.grantPrivilege(PrivilegeType.ALTER);
    tablePrivilege.grantOption(PrivilegeType.DROP);
    Assert.assertEquals(2, tablePrivilege.getPrivileges().size());
    Assert.assertEquals(1, tablePrivilege.getGrantOption().size());
    tablePrivilege.grantOption(PrivilegeType.SELECT);
    Assert.assertEquals(2, tablePrivilege.getGrantOption().size());
    tablePrivilege.revokePrivilege(PrivilegeType.SELECT);
    Assert.assertEquals(2, tablePrivilege.getGrantOption().size());
    Assert.assertEquals(1, tablePrivilege.getPrivileges().size());
    tablePrivilege.grantOption(PrivilegeType.DROP);
    Assert.assertEquals(2, tablePrivilege.getGrantOption().size());
    tablePrivilege.revokeGrantOption(PrivilegeType.DROP);
    Assert.assertEquals(1, tablePrivilege.getGrantOption().size());
  }

  @Test
  public void testTablePrivilegeSerialize() throws IOException {
    TablePrivilege tablePrivilege = new TablePrivilege("test");
    tablePrivilege.grantPrivilege(PrivilegeType.SELECT);
    tablePrivilege.grantPrivilege(PrivilegeType.ALTER);
    tablePrivilege.grantPrivilege(PrivilegeType.DROP);
    tablePrivilege.grantOption(PrivilegeType.SELECT);
    tablePrivilege.grantOption(PrivilegeType.ALTER);
    TablePrivilege tablePrivilege1 = new TablePrivilege();
    ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
    DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
    tablePrivilege.serialize(dataOutputStream);
    tablePrivilege1.deserialize(ByteBuffer.wrap(byteArrayOutputStream.toByteArray()));
    Assert.assertEquals(tablePrivilege, tablePrivilege1);
  }
}
