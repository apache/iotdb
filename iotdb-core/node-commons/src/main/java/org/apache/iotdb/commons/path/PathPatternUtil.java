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

package org.apache.iotdb.commons.path;

import java.util.regex.Pattern;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

public class PathPatternUtil {

  private PathPatternUtil() {}

  /**
   * The input string is a single node of a path pattern. Return true if the node may be a
   * patternNode that can match batch explicit node names. e.g. *, e.g. *, **, d*, *d*.
   */
  public static boolean hasWildcard(String node) {
    return node != null
        && (node.startsWith(ONE_LEVEL_PATH_WILDCARD) || node.endsWith(ONE_LEVEL_PATH_WILDCARD));
  }

  public static boolean isMultiLevelMatchWildcard(String node) {
    return MULTI_LEVEL_PATH_WILDCARD.equals(node);
  }

  /**
   * Determine if a node pattern matches a node name.
   *
   * @param patternNode must be a string starts or ends with *, e.g. *, **, d*, *d*
   * @param nodeName node to match
   */
  public static boolean isNodeMatch(String patternNode, String nodeName) {
    if (patternNode.equals(ONE_LEVEL_PATH_WILDCARD)
        || patternNode.equals(MULTI_LEVEL_PATH_WILDCARD)) {
      return true;
    }
    return Pattern.matches(patternNode.replace("*", ".*"), nodeName);
  }
}
