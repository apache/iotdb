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

package org.apache.iotdb.consensus.raft.core.listener;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * One protocol event on the observation face (the {@code _rec(name, **kw)} records of the Python
 * reference implementation): a name, the emitting node and named fields.
 */
public final class ListenerEvent {

  public final String name;
  public final int node;
  private final Map<String, Object> fields;
  private long step; // set by the harness

  public ListenerEvent(String name, int node, Map<String, Object> fields) {
    this.name = name;
    this.node = node;
    this.fields = Collections.unmodifiableMap(new LinkedHashMap<>(fields));
  }

  public static Builder of(String name, int node) {
    return new Builder(name, node);
  }

  public Object get(String key) {
    return fields.get(key);
  }

  public long getLong(String key) {
    return ((Number) fields.get(key)).longValue();
  }

  public int getInt(String key) {
    return ((Number) fields.get(key)).intValue();
  }

  public String getString(String key) {
    Object v = fields.get(key);
    return v == null ? null : v.toString();
  }

  public boolean has(String key) {
    return fields.containsKey(key);
  }

  public Map<String, Object> fields() {
    return fields;
  }

  public long step() {
    return step;
  }

  public void setStep(long step) {
    this.step = step;
  }

  @Override
  public String toString() {
    return name + "@N" + node + fields;
  }

  public static final class Builder {
    private final String name;
    private final int node;
    private final Map<String, Object> fields = new LinkedHashMap<>();

    Builder(String name, int node) {
      this.name = name;
      this.node = node;
    }

    public Builder with(String key, Object value) {
      fields.put(key, value);
      return this;
    }

    public ListenerEvent build() {
      return new ListenerEvent(name, node, fields);
    }
  }
}
