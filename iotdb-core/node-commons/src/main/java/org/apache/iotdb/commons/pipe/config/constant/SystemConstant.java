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

package org.apache.iotdb.commons.pipe.config.constant;

import org.apache.iotdb.pipe.api.customizer.parameter.PipeParameters;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class SystemConstant {

  public static final String RESTART_KEY = "__system.restart";
  public static final boolean RESTART_DEFAULT_VALUE = false;

  public static final String SQL_DIALECT_KEY = "__system.sql-dialect";
  public static final String SQL_DIALECT_TREE_VALUE = "tree";
  public static final String SQL_DIALECT_TABLE_VALUE = "table";

  /////////////////////////////////// Utility ///////////////////////////////////

  public static final Set<String> SYSTEM_KEYS = new HashSet<>();

  static {
    SYSTEM_KEYS.add(RESTART_KEY);
    SYSTEM_KEYS.add(SQL_DIALECT_KEY);
  }

  public static PipeParameters addSystemKeysIfNecessary(final PipeParameters givenPipeParameters) {
    final Map<String, String> attributes = new HashMap<>(givenPipeParameters.getAttribute());
    attributes.putIfAbsent(SQL_DIALECT_KEY, SQL_DIALECT_TREE_VALUE);
    return new PipeParameters(attributes);
  }

  /////////////////////////////////// Private Constructor ///////////////////////////////////

  private SystemConstant() {
    throw new IllegalStateException("Utility class");
  }
}
