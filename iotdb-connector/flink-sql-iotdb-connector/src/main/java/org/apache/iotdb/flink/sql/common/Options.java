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
package org.apache.iotdb.flink.sql.common;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class Options {
  public static final ConfigOption<String> NODE_URLS =
      ConfigOptions.key("nodeUrls").stringType().defaultValue("127.0.0.1:6667");
  public static final ConfigOption<String> USER =
      ConfigOptions.key("user").stringType().defaultValue("root");
  public static final ConfigOption<String> PASSWORD =
      ConfigOptions.key("password").stringType().defaultValue("root");
  public static final ConfigOption<String> DEVICE =
      ConfigOptions.key("device").stringType().noDefaultValue();
  public static final ConfigOption<Boolean> ALIGNED =
      ConfigOptions.key("aligned").booleanType().defaultValue(false);
  public static final ConfigOption<Integer> LOOKUP_CACHE_MAX_ROWS =
      ConfigOptions.key("lookup.cache.max-rows").intType().defaultValue(-1);
  public static final ConfigOption<Integer> LOOKUP_CACHE_TTL_SEC =
      ConfigOptions.key("lookup.cache.ttl-sec").intType().defaultValue(-1);
  public static final ConfigOption<Long> SCAN_BOUNDED_LOWER_BOUND =
      ConfigOptions.key("scan.bounded.lower-bound").longType().defaultValue(-1L);
  public static final ConfigOption<Long> SCAN_BOUNDED_UPPER_BOUND =
      ConfigOptions.key("scan.bounded.upper-bound").longType().defaultValue(-1L);
}
