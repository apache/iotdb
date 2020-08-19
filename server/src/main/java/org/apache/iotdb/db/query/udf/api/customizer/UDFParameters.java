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

package org.apache.iotdb.db.query.udf.api.customizer;

import java.util.List;
import java.util.Map;
import org.apache.iotdb.tsfile.read.common.Path;

public class UDFParameters {

  private final List<Path> paths;
  private final Map<String, String> attributes;

  public UDFParameters(List<Path> paths, Map<String, String> attributes) {
    this.paths = paths;
    this.attributes = attributes;
  }

  public List<Path> getPaths() {
    return paths;
  }

  public Map<String, String> getAttributes() {
    return attributes;
  }

  public Path getPath(int index) {
    return paths.get(index);
  }

  public String getString(String key) {
    return attributes.get(key);
  }

  public boolean getBoolean(String key) {
    return Boolean.parseBoolean(attributes.get(key));
  }

  public int getInt(String key) {
    return Integer.parseInt(attributes.get(key));
  }

  public long getLong(String key) {
    return Long.parseLong(attributes.get(key));
  }

  public float getFloat(String key) {
    return Float.parseFloat(attributes.get(key));
  }

  public double getDouble(String key) {
    return Double.parseDouble(attributes.get(key));
  }
}
