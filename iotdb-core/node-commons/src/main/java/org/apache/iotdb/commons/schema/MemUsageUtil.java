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

package org.apache.iotdb.commons.schema;

public class MemUsageUtil {

  private MemUsageUtil() {
    // do nothing
  }

  public static long computeStringMemUsage(String value) {
    return estimateStringSize(value);
  }

  /**
   * The basic memory occupied by any BasicMNode object.
   *
   * <ol>
   *   <li>MapEntry in parent
   *       <ol>
   *         <li>key reference, 8B
   *         <li>value reference, 8B
   *         <li>entry size, see ConcurrentHashMap.Node, 28
   *       </ol>
   * </ol>
   */
  public static long computeKVMemUsageInMap(String key, String value) {
    return 40L + estimateStringSize(key) + estimateStringSize(value);
  }

  /**
   * String basic total, 32B
   *
   * <ul>
   *   <li>Object header, 8B
   *   <li>char[] reference + header + length, 8 + 4 + 8= 20B
   *   <li>hash code, 4B
   * </ul>
   */
  private static long estimateStringSize(String string) {
    // each char takes 2B in Java
    return string == null ? 0 : 32 + 2L * string.length();
  }
}
