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
package org.apache.iotdb.commons.utils;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.tsfile.exception.PathParseException;
import org.apache.iotdb.tsfile.read.common.parser.PathNodesGenerator;

public class PathUtils {

  /**
   * @param path the path will split. ex, root.ln.
   * @return string array. ex, [root, ln]
   * @throws IllegalPathException if path isn't correct, the exception will throw
   */
  public static String[] splitPathToDetachedNodes(String path) throws IllegalPathException {
    if ("".equals(path)) {
      return new String[] {};
    }
    try {
      return PathNodesGenerator.splitPathToNodes(path);
    } catch (PathParseException e) {
      throw new IllegalPathException(path);
    }
  }

  public static void isLegalPath(String path) throws IllegalPathException {
    try {
      PathNodesGenerator.splitPathToNodes(path);
    } catch (PathParseException e) {
      throw new IllegalPathException(path);
    }
  }
}
