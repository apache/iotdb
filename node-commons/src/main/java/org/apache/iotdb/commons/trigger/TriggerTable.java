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
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import javax.annotation.concurrent.NotThreadSafe;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

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

  // for getTriggerTable
  public List<TriggerInformation> getAllTriggerInformation() {
    return new ArrayList<>(triggerTable.values());
  }

  public boolean isEmpty() {
    return triggerTable.isEmpty();
  }

  public Map<String, TriggerInformation> getTable() {
    return triggerTable;
  }

  public void serializeTriggerTable(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(triggerTable.size(), outputStream);
    for (TriggerInformation triggerInformation : triggerTable.values()) {
      ReadWriteIOUtils.write(triggerInformation.serialize(), outputStream);
    }
  }

  public void deserializeTriggerTable(InputStream inputStream) throws IOException {
    int size = ReadWriteIOUtils.readInt(inputStream);
    while (size > 0) {
      TriggerInformation triggerInformation = TriggerInformation.deserialize(inputStream);
      triggerTable.put(triggerInformation.getTriggerName(), triggerInformation);
      size--;
    }
  }

  public void clear() {
    triggerTable.clear();
  }
}
