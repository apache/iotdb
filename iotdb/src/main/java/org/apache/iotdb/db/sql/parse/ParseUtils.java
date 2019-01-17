/**
 * Copyright © 2019 Apache IoTDB(incubating) (dev@iotdb.apache.org)
 *
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
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.iotdb.db.sql.parse;

/**
 * Library of utility functions used in the parse code.
 */
public final class ParseUtils {

  private ParseUtils() {
    // prevent instantiation
  }

  /**
   * Performs a descent of the leftmost branch of a tree, stopping when either a node with a
   * non-null token is found or the leaf level is encountered.
   *
   * @param tree candidate node from which to start searching
   * @return node at which descent stopped
   */
  public static AstNode findRootNonNullToken(AstNode tree) {
    while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
      tree = tree.getChild(0);
    }
    return tree;
  }
}
