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

import java.util.ArrayList;

public class StandaloneOnMppEnv extends AbstractEnv {

  private static final Logger logger = LoggerFactory.getLogger(StandaloneOnMppEnv.class);

  private void initEnvironment() throws InterruptedException {
    super.dataNodeWrapperList = new ArrayList<>();
    final String testName = super.getTestClassName() + super.getNextTestCaseString();
    DataNodeWrapper dataNodeWrapper = new StandaloneDataNodeWrapper(null, testName);
    dataNodeWrapper.createDir();
    dataNodeWrapper.changeConfig(ConfigFactory.getConfig().getEngineProperties());
    dataNodeWrapper.start();
    super.dataNodeWrapperList.add(dataNodeWrapper);
    logger.info("In test " + testName + " DataNode " + dataNodeWrapper.getId() + " started.");
    super.testWorking();
  }

  @Override
  public void initBeforeClass() throws InterruptedException {
    logger.debug("=======start init class=======");
    initEnvironment();
  }

  @Override
  public void initBeforeTest() throws InterruptedException {
    logger.debug("=======start init test=======");
    initEnvironment();
  }

  private void cleanupEnvironment() {
    for (DataNodeWrapper dataNodeWrapper : this.dataNodeWrapperList) {
      dataNodeWrapper.stop();
      dataNodeWrapper.waitingToShutDown();
      dataNodeWrapper.destroyDir();
    }
    super.nextTestCase = null;
  }

  @Override
  public void cleanAfterClass() {
    cleanupEnvironment();
  }

  @Override
  public void cleanAfterTest() {
    cleanupEnvironment();
  }
}
