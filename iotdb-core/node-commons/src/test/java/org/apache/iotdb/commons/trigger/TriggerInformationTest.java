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

package org.apache.iotdb.commons.trigger;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.confignode.rpc.thrift.TTriggerState;
import org.apache.iotdb.trigger.api.enums.FailureStrategy;
import org.apache.iotdb.trigger.api.enums.TriggerEvent;

import org.junit.Assert;
import org.junit.Test;

import java.util.Collections;

public class TriggerInformationTest {

  @Test
  public void equalsHashCodeIncludesUriUsageAndFailureStrategy() throws IllegalPathException {
    TriggerInformation triggerInformation =
        createTriggerInformation(true, FailureStrategy.OPTIMISTIC);
    TriggerInformation sameTriggerInformation =
        createTriggerInformation(true, FailureStrategy.OPTIMISTIC);

    Assert.assertEquals(triggerInformation, sameTriggerInformation);
    Assert.assertEquals(triggerInformation.hashCode(), sameTriggerInformation.hashCode());

    Assert.assertNotEquals(
        triggerInformation, createTriggerInformation(true, FailureStrategy.PESSIMISTIC));
    Assert.assertNotEquals(
        triggerInformation, createTriggerInformation(false, FailureStrategy.OPTIMISTIC));
  }

  private TriggerInformation createTriggerInformation(
      boolean isUsingURI, FailureStrategy failureStrategy) throws IllegalPathException {
    return new TriggerInformation(
        new PartialPath("root.test.**"),
        "trigger",
        "trigger.class",
        isUsingURI,
        "trigger.jar",
        Collections.singletonMap("k", "v"),
        TriggerEvent.AFTER_INSERT,
        TTriggerState.INACTIVE,
        false,
        null,
        failureStrategy,
        "jar-md5");
  }
}
