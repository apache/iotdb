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

import org.apache.iotdb.confignode.rpc.thrift.TTriggerState;

import javax.annotation.concurrent.NotThreadSafe;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

/** This Class used to save the information of Triggers and implements methods of manipulate it. */
@NotThreadSafe
public class TriggerTable {
  private final Map<String, TriggerInformation> triggerTable;

  public TriggerTable() {
    triggerTable = new ConcurrentHashMap<>();
  }

  public TriggerTable(Map<String, TriggerInformation> triggerTable) {
    this.triggerTable = triggerTable;
  }
  // for createTrigger
  public void addTriggerInformation(String triggerName, TriggerInformation triggerInformation) {
    triggerTable.put(triggerName, triggerInformation);
  }

  public TriggerInformation getTriggerInformation(String triggerName) {
    return triggerTable.get(triggerName);
  }

  public TriggerInformation removeTriggerInformation(String triggerName) {
    return triggerTable.remove(triggerName);
  }

  public void setTriggerInformation(String triggerName, TriggerInformation triggerInformation) {
    triggerTable.put(triggerName, triggerInformation);
  }

  // for dropTrigger
  public void deleteTriggerInformation(String triggerName) {
    triggerTable.remove(triggerName);
  }

  public boolean containsTrigger(String triggerName) {
    return triggerTable.containsKey(triggerName);
  }

  public void setTriggerState(String triggerName, TTriggerState triggerState) {
    triggerTable.get(triggerName).setTriggerState(triggerState);
  }

  // for showTrigger
  public Map<String, TTriggerState> getAllTriggerStates() {
    Map<String, TTriggerState> allTriggerStates = new HashMap<>(triggerTable.size());
    triggerTable.forEach((k, v) -> allTriggerStates.put(k, v.getTriggerState()));
    return allTriggerStates;
  }
  // for getTriggerTable
  public List<TriggerInformation> getAllTriggerInformation() {
    return triggerTable.values().stream().collect(Collectors.toList());
  }

  public boolean isEmpty() {
    return triggerTable.isEmpty();
  }

  public Map<String, TriggerInformation> getTable() {
    return triggerTable;
  }
}
