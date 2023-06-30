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

package org.apache.iotdb.db.trigger.service;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.trigger.TriggerInformation;
import org.apache.iotdb.commons.trigger.exception.TriggerManagementException;
import org.apache.iotdb.confignode.rpc.thrift.TTriggerState;
import org.apache.iotdb.trigger.api.enums.FailureStrategy;
import org.apache.iotdb.trigger.api.enums.TriggerEvent;

import org.junit.Assert;
import org.junit.Test;

import java.util.List;

public class TriggerManagementServiceTest {

  private static final TriggerManagementService SERVICE = TriggerManagementService.getInstance();

  @Test
  public void testRegisterAndActiveAndInactiveAndDrop() {
    try {
      TriggerInformation triggerInformation =
          new TriggerInformation(
              new PartialPath("root.test.**"),
              "test1",
              "org.apache.iotdb.db.trigger.service.TestTrigger",
              false,
              "test1.jar",
              null,
              TriggerEvent.AFTER_INSERT,
              TTriggerState.INACTIVE,
              false,
              null,
              FailureStrategy.OPTIMISTIC,
              "testMD5test");
      List<TriggerInformation> triggerInformationList =
          SERVICE.getAllTriggerInformationInTriggerTable();
      Assert.assertEquals(0, triggerInformationList.size());

      try {
        SERVICE.doRegister(triggerInformation, false);
      } catch (TriggerManagementException e) {
        if (!e.getMessage().contains("reflect")) {
          Assert.fail();
        }
      }

      triggerInformationList = SERVICE.getAllTriggerInformationInTriggerTable();
      Assert.assertEquals(1, triggerInformationList.size());
      Assert.assertEquals(triggerInformation, triggerInformationList.get(0));

      Assert.assertEquals(TTriggerState.INACTIVE, triggerInformationList.get(0).getTriggerState());
      SERVICE.activeTrigger("test1");
      Assert.assertEquals(
          TTriggerState.ACTIVE,
          SERVICE.getAllTriggerInformationInTriggerTable().get(0).getTriggerState());
      SERVICE.inactiveTrigger("test1");
      Assert.assertEquals(
          TTriggerState.INACTIVE,
          SERVICE.getAllTriggerInformationInTriggerTable().get(0).getTriggerState());

      SERVICE.dropTrigger("test1", false);
      Assert.assertEquals(0, SERVICE.getAllTriggerInformationInTriggerTable().size());
    } catch (Exception e) {
      e.printStackTrace();
      Assert.fail();
    }
  }
}
