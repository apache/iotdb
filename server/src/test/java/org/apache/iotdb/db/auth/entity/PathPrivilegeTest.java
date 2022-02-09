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

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class PathPrivilegeTest {

  @Test
  public void testPathPrivilege() {
    PathPrivilege pathPrivilege = new PathPrivilege();
    pathPrivilege.setPath("root.ln");
    pathPrivilege.setPrivileges(Collections.singleton(1));
    Assert.assertEquals("root.ln : INSERT_TIMESERIES", pathPrivilege.toString());
    PathPrivilege pathPrivilege1 = new PathPrivilege();
    pathPrivilege1.setPath("root.sg");
    pathPrivilege1.setPrivileges(Collections.singleton(1));
    Assert.assertNotEquals(pathPrivilege, pathPrivilege1);
    pathPrivilege.deserialize(pathPrivilege1.serialize());
    Assert.assertEquals("root.sg : INSERT_TIMESERIES", pathPrivilege.toString());
  }
}
