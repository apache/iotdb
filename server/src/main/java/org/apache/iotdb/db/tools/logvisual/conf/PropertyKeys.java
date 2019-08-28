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

package org.apache.iotdb.db.tools.logvisual.conf;

/**
 * PropertyKeys holds the keys of properties that store the paths chosen by the user the last
 * time he uses this tool for convenience.
 */
public enum PropertyKeys {
  /**
   * The last path of the parser property file chosen by the user.
   */
  DEFAULT_PARSER_FILE_PATH("parser_properties_path"),
  /**
   * The last path of the log file chosen by the user.
   */
  DEFAULT_LOG_FILE_PATH("log_path"),
  /**
   * The last path of the visualization plan file chosen by the user.
   */
  DEFAULT_PLAN_PATH("plans_path");
  private String key;

  PropertyKeys(String key) {
    this.key = key;
  }

  public String getKey() {
    return key;
  }
}