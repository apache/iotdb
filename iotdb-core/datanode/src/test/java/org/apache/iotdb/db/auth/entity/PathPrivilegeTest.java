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
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class PathPrivilegeTest {

  @Test
  public void testPathPrivilege() throws IllegalPathException {
    PathPrivilege pathPrivilege = new PathPrivilege();
    pathPrivilege.setPath(new PartialPath("root.ln"));
    pathPrivilege.setPrivileges(Collections.singleton(1));
    Assert.assertEquals("root.ln : WRITE_DATA", pathPrivilege.toString());
    PathPrivilege pathPrivilege1 = new PathPrivilege();
    pathPrivilege1.setPath(new PartialPath("root.sg"));
    pathPrivilege1.setPrivileges(Collections.singleton(1));
    Assert.assertNotEquals(pathPrivilege, pathPrivilege1);
    pathPrivilege.deserialize(pathPrivilege1.serialize());
    Assert.assertEquals("root.sg : WRITE_DATA", pathPrivilege.toString());
  }
}
