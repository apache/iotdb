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

package org.apache.iotdb.commons.i18n;

public final class PathMessages {

  // PartialPath
  public static final String WILDCARDS_NOT_ALLOWED_IN_PREFIX =
      "Wildcards are not allowed in the prefix path: %s";
  public static final String ONLY_MEASUREMENT_PATH_SUPPORT_ALIAS =
      "Only MeasurementPath support alias";
  public static final String ENCOUNTERED_ILLEGAL_PATH = "Encountered an illegal path {}";

  // AlignedPath / MeasurementPath
  public static final String NODES_LENGTH_SHOULD_BE_GREATER_THAN_ONE =
      "nodes.length for MeasurementPath should always be greater than 1, current is: %s";
  public static final String NODES_LENGTH_SHOULD_BE_GREATER_THAN_TWO =
      "nodes.length for MeasurementPath should always be greater than 2, current is: %s";
  public static final String ALIGNED_PATH_NO_MEASUREMENT_NAME =
      "AlignedPath doesn't have measurement name!";
  public static final String PATH_IS_ILLEGAL = "path is illegal: {}";

  // MeasurementPath deserialization
  public static final String UNKNOWN_MEASUREMENT_SCHEMA_TYPE =
      "Type (%s) of measurementSchema is unknown.";

  // PathDeserializeUtil
  public static final String INVALID_PATH_TYPE = "Invalid path type: %s";

  // IFullPath
  public static final String ONLY_ACCEPT_MEASUREMENT_AND_ALIGNED =
      "Only accept MeasurementPath and AlignedPath.";

  private PathMessages() {}
  // ---------------------------------------------------------------------------
  // Additional auto-collected messages
  // ---------------------------------------------------------------------------
  public static final String EXCEPTION_NODES_LENGTH_MEASUREMENTPATH_SHOULD_ALWAYS_GREATER_THAN_2_CURRENT_39B913AE = "nodes.length for MeasurementPath should always be greater than 2, current is: ";
  public static final String EXCEPTION_WILDCARDS_NOT_ALLOWED_PREFIX_PATH_948C42D1 = "Wildcards are not allowed in the prefix path: ";

}
