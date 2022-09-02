/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.commons.trigger;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/** This Class used to save the information of Triggers and implements methods of manipulate it. */
public class TriggerTable {
  private final Map<String, TriggerInformation> triggerTable;

  public TriggerTable() {
    triggerTable = new ConcurrentHashMap<>();
  }

  public TriggerTable(Map<String, TriggerInformation> triggerTable) {
    this.triggerTable = triggerTable;
  }

  public void addTriggerInformation(String triggerName, TriggerInformation triggerInformation) {
    triggerTable.put(triggerName, triggerInformation);
  }

  public void deleteTriggerInformation(String triggerName) {
    triggerTable.remove(triggerName);
  }

  public Map getAllTriggerStates() {
    // After we get the size of TriggerTable, maybe more values has put in it before we put all
    // TriggerStates in map of allTriggerStates.
    // Mostly we create only one Trigger at a time, so the initial size is 'triggerTable.size() +
    // 1' here.
    Map allTriggerStates = new HashMap(triggerTable.size() + 1);
    triggerTable.forEach((k, v) -> allTriggerStates.put(k, v.getTriggerState()));

    return allTriggerStates;
  }

  public Map getTable() {
    return triggerTable;
  }
}
