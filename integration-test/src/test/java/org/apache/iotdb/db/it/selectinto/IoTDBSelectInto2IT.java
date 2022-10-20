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

package org.apache.iotdb.db.it.selectinto;

import org.apache.iotdb.it.env.ConfigFactory;
import org.apache.iotdb.it.env.EnvFactory;
import org.apache.iotdb.it.framework.IoTDBTestRunner;
import org.apache.iotdb.itbase.category.ClusterIT;

import org.junit.After;
import org.junit.Before;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

import static org.apache.iotdb.db.it.utils.TestUtils.prepareData;

@RunWith(IoTDBTestRunner.class)
// TODO add LocalStandaloneIT back while deleting old standalone
@Category({ClusterIT.class})
public class IoTDBSelectInto2IT extends IoTDBSelectIntoIT {

  private static int numOfPointsPerPage;

  @Before
  public void setUp() throws Exception {
    selectIntoInsertTabletPlanRowLimit =
        ConfigFactory.getConfig().getSelectIntoInsertTabletPlanRowLimit();
    numOfPointsPerPage = ConfigFactory.getConfig().getMaxNumberOfPointsInPage();
    ConfigFactory.getConfig().setSelectIntoInsertTabletPlanRowLimit(8);
    ConfigFactory.getConfig().setMaxNumberOfPointsInPage(5);
    EnvFactory.getEnv().initBeforeTest();
    prepareData(SQLs);
  }

  @After
  public void tearDown() throws Exception {
    EnvFactory.getEnv().cleanAfterTest();
    ConfigFactory.getConfig()
        .setSelectIntoInsertTabletPlanRowLimit(selectIntoInsertTabletPlanRowLimit);
    ConfigFactory.getConfig().setMaxNumberOfPointsInPage(numOfPointsPerPage);
  }
}
