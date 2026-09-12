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

package org.apache.iotdb.cli.i18n;

public final class FsParserMessages {
  private FsParserMessages() {}

  public static final String WC_MODE = "wc supports only -c";
  public static final String OUTPUT_FORMAT = "Invalid output format: %s";
  public static final String EXCLUSIVE_SCOPE = "Options -d and -t are mutually exclusive";
  public static final String TAG_MATCH_FILTER = "--tag-match requires --tag-filter";
  public static final String TAG_MATCH_VALUE = "Invalid --tag-match: %s";
  public static final String TAG_MATCH_COUNT = "--tag-match requires at least two tag filters";
  public static final String TAG_FILTER_MATCH =
      "two or more tag filters require --tag-match all or any";
  public static final String OPTION_VALUE = "Invalid value for %s: %s";
  public static final String TAG_OPERATOR = "Invalid --tag-filter operator: %s";
  public static final String DUPLICATE_MEASUREMENT = "measurement '%s' specified more than once";
}
