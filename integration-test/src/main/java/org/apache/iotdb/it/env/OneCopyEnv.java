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
package org.apache.iotdb.it.env;

import org.apache.iotdb.it.framework.IoTDBTestLogger;

import org.slf4j.Logger;

public class OneCopyEnv extends AbstractEnv {

  private static final Logger logger = IoTDBTestLogger.logger;

  @Override
  public void initBeforeClass() throws InterruptedException {
    logger.debug("=======start init class=======");
    super.initEnvironment(1, 1);
  }

  @Override
  public void initClusterEnvironment(int configNodesNum, int dataNodesNum) {
    // Do nothing
  }

  @Override
  public void initBeforeTest() {
    logger.debug("=======start init test=======");
    initEnvironment(1, 1);
  }
}
