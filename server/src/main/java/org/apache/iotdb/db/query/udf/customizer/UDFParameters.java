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

package org.apache.iotdb.db.query.udf.customizer;

import org.apache.iotdb.tsfile.read.common.Path;

/**
 * This class helps the user parse UDF parameters in {@code SELECT} statements.
 * A UDF parameter can be a single-quoted string, a double-quoted string or a suffix path.
 * The index of the UDF parameter starts at 0.
 * <br/>
 * {@code SELECT udf(wt01, wt02, "1000", "123.45") FROM root.ln.wf01;}
 * <br/>
 * For example, in the {@code SELECT} statement above, there are 4 UDF parameters. The first 2 are
 * suffix paths, and the last 2 are double-quoted strings.
 */
public class UDFParameters {

  private final String[] parameters;
  private final Path[] paths;

  /**
   * @param parameters a {@code String} element in parameters will be {@code null} if the
   *                   corresponding parameter is a suffix path
   * @param paths      a {@code Path} element in paths will be {@code null} if the corresponding
   *                   parameter is a single-quoted string or a double-quoted string
   */
  public UDFParameters(String[] parameters, Path[] paths) {
    assert parameters.length == paths.length;
    this.parameters = parameters;
    this.paths = paths;
  }

  public int length() {
    return parameters.length;
  }

  public Path getPath(int index) {
    return paths[index];
  }

  public String getString(int index) {
    return parameters[index];
  }

  public boolean getBoolean(int index) {
    return Boolean.parseBoolean(parameters[index]);
  }

  public int getInt(int index) {
    return Integer.parseInt(parameters[index]);
  }

  public long getLong(int index) {
    return Long.parseLong(parameters[index]);
  }

  public float getFloat(int index) {
    return Float.parseFloat(parameters[index]);
  }

  public double getDouble(int index) {
    return Double.parseDouble(parameters[index]);
  }
}
