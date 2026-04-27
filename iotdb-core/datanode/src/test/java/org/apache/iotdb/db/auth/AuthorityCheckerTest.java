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

package org.apache.iotdb.db.auth;

import org.apache.iotdb.commons.auth.entity.PrivilegeType;
import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.MeasurementPath;

import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;

public class AuthorityCheckerTest {

  @Test
  public void testLogReduce() throws IllegalPathException {
    final CommonConfig config = CommonDescriptor.getInstance().getConfig();
    final int oldSize = config.getPathLogMaxSize();
    config.setPathLogMaxSize(1);
    Assert.assertEquals(
        "No permissions for this operation, please add privilege WRITE_DATA on [root.db.device.s1, ...]",
        AuthorityChecker.getTSStatus(
                Arrays.asList(0, 1),
                Arrays.asList(
                    new MeasurementPath("root.db.device.s1"),
                    new MeasurementPath("root.db.device.s2")),
                PrivilegeType.WRITE_DATA)
            .getMessage());
    config.setPathLogMaxSize(oldSize);
  }
}
