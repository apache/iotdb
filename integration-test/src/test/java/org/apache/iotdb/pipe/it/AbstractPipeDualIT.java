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

package org.apache.iotdb.pipe.it;

import org.apache.iotdb.it.env.MultiEnvFactory;
import org.apache.iotdb.itbase.env.BaseEnv;
import org.apache.iotdb.pipe.PipeEnvironmentException;

import org.junit.After;
import org.junit.Before;

abstract class AbstractPipeDualIT {

  protected BaseEnv senderEnv;
  protected BaseEnv receiverEnv;

  @Before
  public void setUp() throws PipeEnvironmentException {
    try {
      MultiEnvFactory.createEnv(2);
      senderEnv = MultiEnvFactory.getEnv(0);
      receiverEnv = MultiEnvFactory.getEnv(1);

      senderEnv.getConfig().getCommonConfig().setAutoCreateSchemaEnabled(true);
      receiverEnv.getConfig().getCommonConfig().setAutoCreateSchemaEnabled(true);

      senderEnv.initClusterEnvironment();
      receiverEnv.initClusterEnvironment();
    } catch (Exception | Error e) {
      throw new PipeEnvironmentException(e.getMessage(), e);
    }
  }

  @After
  public void tearDown() throws PipeEnvironmentException {
    try {
      senderEnv.cleanClusterEnvironment();
      receiverEnv.cleanClusterEnvironment();
    } catch (Exception | Error e) {
      throw new PipeEnvironmentException(e.getMessage(), e);
    }
  }
}
