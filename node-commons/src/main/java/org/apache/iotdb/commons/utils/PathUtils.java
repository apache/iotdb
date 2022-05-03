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
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import org.apache.commons.lang.StringEscapeUtils;

import java.util.ArrayList;
import java.util.List;

public class PathUtils {

  /**
   * @param path the path will split. ex, root.ln.
   * @return string array. ex, [root, ln]
   * @throws IllegalPathException if path isn't correct, the exception will throw
   */
  public static String[] splitPathToDetachedPath(String path) throws IllegalPathException {
    // NodeName is treated as identifier. When parsing identifier, unescapeJava is called.
    // Therefore we call unescapeJava here.
    path = StringEscapeUtils.unescapeJava(path);
    if (path.endsWith(TsFileConstant.PATH_SEPARATOR)) {
      throw new IllegalPathException(path);
    }
    List<String> nodes = new ArrayList<>();
    int startIndex = 0;
    int endIndex;
    int length = path.length();
    for (int i = 0; i < length; i++) {
      if (path.charAt(i) == TsFileConstant.PATH_SEPARATOR_CHAR) {
        String node = path.substring(startIndex, i);
        if (node.isEmpty()) {
          throw new IllegalPathException(path);
        }
        nodes.add(node);
        startIndex = i + 1;
      } else if (path.charAt(i) == TsFileConstant.BACK_QUOTE) {
        startIndex = i + 1;
        endIndex = path.indexOf(TsFileConstant.BACK_QUOTE, startIndex);
        if (endIndex == -1) {
          // single '`', like root.sg.`s
          throw new IllegalPathException(path);
        }
        while (endIndex != -1 && endIndex != length - 1) {
          char afterQuote = path.charAt(endIndex + 1);
          if (afterQuote == TsFileConstant.BACK_QUOTE) {
            // for example, root.sg.```
            if (endIndex == length - 2) {
              throw new IllegalPathException(path);
            }
            endIndex = path.indexOf(TsFileConstant.BACK_QUOTE, endIndex + 2);
          } else if (afterQuote == '.') {
            break;
          } else {
            throw new IllegalPathException(path);
          }
        }
        // replace `` with ` in a quoted identifier
        String node = path.substring(startIndex, endIndex).replace("``", "`");
        if (node.isEmpty()) {
          throw new IllegalPathException(path);
        }

        nodes.add(node);
        // skip the '.' after '`'
        i = endIndex + 1;
        startIndex = endIndex + 2;
      }
    }
    // last node
    if (startIndex <= path.length() - 1) {
      String node = path.substring(startIndex);
      if (node.isEmpty()) {
        throw new IllegalPathException(path);
      }
      nodes.add(node);
    }
    return nodes.toArray(new String[0]);
  }
}
