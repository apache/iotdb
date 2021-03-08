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

package org.apache.iotdb.db.engine.trigger.example;

import org.apache.iotdb.db.engine.trigger.api.Trigger;
import org.apache.iotdb.db.engine.trigger.api.TriggerAttributes;
import org.apache.iotdb.tsfile.utils.Binary;

import java.util.HashMap;
import java.util.Map;

public class Counter implements Trigger {

  public static int BASE = 1377;

  private int counter = BASE;
  private boolean isStopped = true;
  private int migrationTimes = 0;

  @Override
  public void onStart(TriggerAttributes attributes) {
    counter = attributes.getIntOrDefault("base", BASE);
    isStopped = false;
    migrationTimes = 0;
  }

  @Override
  public void onStop() {
    counter = BASE;
    isStopped = true;
    migrationTimes = 0;
  }

  @Override
  public Map<String, Object> migrateToNew() {
    Map<String, Object> objects = new HashMap<>();
    objects.put("counter", counter);
    objects.put("isStopped", isStopped);
    objects.put("migrationTimes", migrationTimes);
    return objects;
  }

  @Override
  public void migrateFromOld(Map<String, Object> objects) {
    counter = (int) objects.get("counter");
    isStopped = (boolean) objects.get("isStopped");
    migrationTimes = (int) objects.get("migrationTimes") + 1;
  }

  @Override
  public Integer fire(long timestamp, Integer value) {
    ++counter;
    return value;
  }

  @Override
  public Long fire(long timestamp, Long value) {
    ++counter;
    return value;
  }

  @Override
  public Float fire(long timestamp, Float value) {
    ++counter;
    return value;
  }

  @Override
  public Double fire(long timestamp, Double value) {
    ++counter;
    return value;
  }

  @Override
  public Boolean fire(long timestamp, Boolean value) {
    ++counter;
    return value;
  }

  @Override
  public Binary fire(long timestamp, Binary value) {
    ++counter;
    return value;
  }

  public void setCounter(int counter) {
    this.counter = counter;
  }

  public int getCounter() {
    return counter;
  }

  public boolean isStopped() {
    return isStopped;
  }

  public int getMigrationTimes() {
    return migrationTimes;
  }
}
