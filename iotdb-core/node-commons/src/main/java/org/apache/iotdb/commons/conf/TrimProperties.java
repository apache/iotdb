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
package org.apache.iotdb.commons.conf;

import java.util.Optional;
import java.util.Properties;

public class TrimProperties extends Properties {
  @Override
  public synchronized Object get(Object key) {
    Object value = super.get(key);
    if (value instanceof String) {
      return ((String) value).trim();
    }
    return value;
  }

  @Override
  public synchronized Object put(Object key, Object value) {
    if (value instanceof String) {
      value = ((String) value).trim();
    }
    return super.put(key, value);
  }

  @Override
  public synchronized String getProperty(String key, String defaultValue) {
    String val = getProperty(key);
    if (defaultValue != null) {
      defaultValue = defaultValue.trim();
    }
    return Optional.ofNullable(val).map(String::trim).orElse(defaultValue);
  }
}
