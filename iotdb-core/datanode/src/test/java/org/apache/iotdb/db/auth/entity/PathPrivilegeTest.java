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

import org.apache.iotdb.commons.auth.entity.PathPrivilege;
import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;

import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;

public class PathPrivilegeTest {

  @Test
  public void testPathPrivilege_Init() throws IllegalPathException, IOException {
    PathPrivilege pathPrivilege = new PathPrivilege();
    pathPrivilege.setPath(new PartialPath("root.ln"));
    pathPrivilege.setPrivileges(Collections.singleton(PrivilegeType.WRITE_DATA));
    pathPrivilege.setGrantOpt(new HashSet<>());
    Assert.assertEquals("root.ln : WRITE_DATA", pathPrivilege.toString());
    PathPrivilege pathPrivilege1 = new PathPrivilege();
    pathPrivilege1.setPath(new PartialPath("root.sg"));
    pathPrivilege1.setPrivileges(Collections.singleton(PrivilegeType.WRITE_DATA));
    pathPrivilege1.setGrantOpt(new HashSet<>());
    Assert.assertNotEquals(pathPrivilege, pathPrivilege1);
    pathPrivilege.deserialize(pathPrivilege1.serialize());
    Assert.assertEquals("root.sg : WRITE_DATA", pathPrivilege.toString());
  }

  @Test
  public void testPathPrivilege_GrantAndRevoke() throws IllegalPathException {
    PathPrivilege pathPrivilege = new PathPrivilege(new PartialPath("root.ln"));

    pathPrivilege.grantPrivilege(PrivilegeType.READ_DATA, false);
    pathPrivilege.grantPrivilege(PrivilegeType.READ_SCHEMA, true);
    pathPrivilege.grantPrivilege(PrivilegeType.WRITE_SCHEMA, true);

    Assert.assertEquals(pathPrivilege.getPrivilegeIntSet().size(), 3);
    Assert.assertEquals(pathPrivilege.getGrantOpt().size(), 2);

    Assert.assertTrue(pathPrivilege.getGrantOpt().contains(PrivilegeType.READ_SCHEMA));
    Assert.assertTrue(pathPrivilege.getGrantOpt().contains(PrivilegeType.WRITE_SCHEMA));

    pathPrivilege.revokePrivilege(PrivilegeType.READ_SCHEMA);
    Assert.assertFalse(pathPrivilege.revokePrivilege(PrivilegeType.READ_SCHEMA));

    HashSet<PrivilegeType> privs = new HashSet<>();
    privs.add(PrivilegeType.WRITE_SCHEMA);
    Assert.assertEquals(pathPrivilege.getGrantOpt(), privs);

    privs.add(PrivilegeType.READ_DATA);
    Assert.assertEquals(pathPrivilege.getPrivileges(), privs);

    Assert.assertFalse(pathPrivilege.revokeGrantOpt(PrivilegeType.READ_SCHEMA));
    Assert.assertTrue(pathPrivilege.revokeGrantOpt(PrivilegeType.WRITE_SCHEMA));
    Assert.assertEquals(pathPrivilege.getGrantOpt().size(), 0);
  }

  @Test
  public void testPrivilegePath_GetAllPrivilegeMask() throws IllegalPathException {

    PathPrivilege pathPrivilege = new PathPrivilege(new PartialPath("root.ln"));

    pathPrivilege.grantPrivilege(PrivilegeType.READ_DATA, false);
    pathPrivilege.grantPrivilege(PrivilegeType.READ_SCHEMA, true);
    pathPrivilege.grantPrivilege(PrivilegeType.WRITE_SCHEMA, true);
    // mask as : 0000-0000-0000-1100|0000-0000-0000-1101
    Assert.assertEquals(pathPrivilege.getAllPrivileges(), (0b11 << (2 + 16)) | (0b1101));

    PathPrivilege pathPrivilege2 = new PathPrivilege(new PartialPath("root.ln"));
    pathPrivilege2.setAllPrivileges((0b11 << (2 + 16)) | (0b1101));
    Assert.assertEquals(pathPrivilege, pathPrivilege2);
  }
}
