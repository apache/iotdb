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
import org.apache.iotdb.tsfile.exception.NotImplementedException;
import org.apache.iotdb.tsfile.utils.Binary;

import java.util.HashMap;
import java.util.Map;

public class Accumulator implements Trigger {

  public static int BASE = 7713;

  private double accumulator = BASE;
  private boolean isStopped = true;

  @Override
  public void onStart(TriggerAttributes attributes) {
    accumulator = attributes.getDoubleOrDefault("base", BASE);
    isStopped = false;
  }

  @Override
  public void onStop() {
    accumulator = BASE;
    isStopped = true;
  }

  @Override
  public Map<String, Object> migrateToNew() {
    Map<String, Object> objects = new HashMap<>();
    objects.put("accumulator", accumulator);
    objects.put("isStopped", isStopped);
    return objects;
  }

  @Override
  public void migrateFromOld(Map<String, Object> objects) {
    accumulator = (double) objects.get("accumulator");
    isStopped = (boolean) objects.get("isStopped");
  }

  @Override
  public Integer fire(long timestamp, Integer value) {
    accumulator += value;
    return value;
  }

  @Override
  public Long fire(long timestamp, Long value) {
    accumulator += value;
    return value;
  }

  @Override
  public Float fire(long timestamp, Float value) {
    accumulator += value;
    return value;
  }

  @Override
  public Double fire(long timestamp, Double value) {
    accumulator += value;
    return value;
  }

  @Override
  public Boolean fire(long timestamp, Boolean value) {
    throw new NotImplementedException();
  }

  @Override
  public Binary fire(long timestamp, Binary value) {
    throw new NotImplementedException();
  }

  public double getAccumulator() {
    return accumulator;
  }

  public boolean isStopped() {
    return isStopped;
  }
}
