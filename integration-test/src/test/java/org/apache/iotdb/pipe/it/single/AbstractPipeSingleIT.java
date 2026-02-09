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

package org.apache.iotdb.pipe.it.single;

import org.apache.iotdb.db.it.utils.TestUtils;
import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.itbase.env.BaseEnv;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.util.Arrays;

abstract class AbstractPipeSingleIT {

  protected static ThreadLocal<BaseEnv> envContainer;
  protected BaseEnv env;

  @BeforeClass
  public static void setUp() {
    MultiEnvFactory.createEnv(1);
    envContainer.set(MultiEnvFactory.getEnv(0));
    envContainer
        .get()
        .getConfig()
        .getCommonConfig()
        .setAutoCreateSchemaEnabled(true)
        .setPipeMemoryManagementEnabled(false)
        .setIsPipeEnableMemoryCheck(false)
        .setPipeAutoSplitFullEnabled(false);
    envContainer.get().initClusterEnvironment();
  }

  @AfterClass
  public static void tearDown() {
    envContainer.get().cleanClusterEnvironment();
  }

  @Before
  public void setEnv() {
    env = envContainer.get();
  }

  @After
  public final void cleanEnvironment() {
    TestUtils.executeNonQueries(env, Arrays.asList("drop database root.**"), null);
  }
}
