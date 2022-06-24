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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;

public class StandaloneOnMppEnv extends AbstractEnv {

  private static final Logger logger = LoggerFactory.getLogger(StandaloneOnMppEnv.class);

  private void initEnvironment() {
    DataNodeWrapper dataNodeWrapper =
        new StandaloneDataNodeWrapper(
            null, super.getTestClassName(), super.getTestMethodName(), searchAvailablePorts());
    dataNodeWrapper.createDir();
    dataNodeWrapper.changeConfig(ConfigFactory.getConfig().getEngineProperties());
    dataNodeWrapper.start();
    super.dataNodeWrapperList = Collections.singletonList(dataNodeWrapper);
    super.testWorking();
  }

  @Override
  public void initBeforeClass() throws InterruptedException {
    logger.debug("=======start init class=======");
    initEnvironment();
  }

  @Override
  public void initBeforeTest() {
    logger.debug("=======start init test=======");
    initEnvironment();
  }
}
