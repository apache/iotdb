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

package org.apache.iotdb.metrics.utils;

// universal tag related to system
public enum SystemTag {
  TYPE("type"),
  NAME("name"),
  FROM("from"),
  POOL("pool"),
  MODULE("module"),
  DISK("disk_id"),
  ID("id"),
  AREA("area"),
  COMPILER("compiler"),
  STATE("state"),
  IFACE_NAME("iface_name"),
  PROCESS_NAME("process_num");

  final String value;

  SystemTag(String value) {
    this.value = value;
  }

  @Override
  public String toString() {
    return value;
  }
}
