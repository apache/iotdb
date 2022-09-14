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
package org.apache.iotdb.lsm.context;

import org.apache.iotdb.lsm.strategy.PreOrderAccessStrategy;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class InsertContext extends Context {

  List<Object> keys;

  Object value;

  public InsertContext() {
    super();
    type = ContextType.INSERT;
    accessStrategy = new PreOrderAccessStrategy();
  }

  public InsertContext(Object value, Object... keys) {
    super();
    this.value = value;
    this.keys = new ArrayList<>();
    this.keys.addAll(Arrays.asList(keys));
    type = ContextType.INSERT;
    accessStrategy = new PreOrderAccessStrategy();
  }

  public Object getKey() {
    return keys.get(level);
  }

  public void setKeys(List<Object> keys) {
    this.keys = keys;
  }

  public void setValue(Object value) {
    this.value = value;
  }

  public List<Object> getKeys() {
    return keys;
  }

  public Object getValue() {
    return value;
  }
}
