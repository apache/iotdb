/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.tsfile.read.common.parser;

import org.apache.iotdb.db.qp.sql.PathParser;
import org.apache.iotdb.db.qp.sql.PathParser.NodeNameContext;
import org.apache.iotdb.db.qp.sql.PathParserBaseVisitor;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;

import org.apache.commons.lang3.math.NumberUtils;

import java.util.List;

public class PathVisitor extends PathParserBaseVisitor<String[]> {

  @Override
  public String[] visitPath(PathParser.PathContext ctx) {
    if (ctx.prefixPath() != null) {
      return visitPrefixPath(ctx.prefixPath());
    } else {
      return visitSuffixPath(ctx.suffixPath());
    }
  }

  @Override
  public String[] visitPrefixPath(PathParser.PrefixPathContext ctx) {
    List<NodeNameContext> nodeNames = ctx.nodeName();
    String[] path = new String[nodeNames.size() + 1];
    path[0] = ctx.ROOT().getText();
    for (int i = 0; i < nodeNames.size(); i++) {
      path[i + 1] = parseNodeName(nodeNames.get(i));
    }
    return path;
  }

  @Override
  public String[] visitSuffixPath(PathParser.SuffixPathContext ctx) {
    List<NodeNameContext> nodeNames = ctx.nodeName();
    String[] path = new String[nodeNames.size()];
    for (int i = 0; i < nodeNames.size(); i++) {
      path[i] = parseNodeName(nodeNames.get(i));
    }
    return path;
  }

  private String parseNodeName(PathParser.NodeNameContext ctx) {
    String nodeName = ctx.getText();
    if (nodeName.startsWith(TsFileConstant.BACK_QUOTE_STRING)
        && nodeName.endsWith(TsFileConstant.BACK_QUOTE_STRING)) {
      String unWrapped = nodeName.substring(1, nodeName.length() - 1);
      if (isRealNumber(unWrapped)
          || !TsFileConstant.IDENTIFIER_PATTERN.matcher(unWrapped).matches()) {
        return nodeName;
      }
      return unWrapped;
    }
    return nodeName;
  }

  /** Return true if the str is a real number. Examples: 1.0; +1.0; -1.0; 0011; 011e3; +23e-3 */
  public static boolean isRealNumber(String str) {
    if (str.startsWith("+") || str.startsWith("-")) {
      String removeSign = str.substring(1);
      if (removeSign.startsWith("+") || removeSign.startsWith("-")) {
        return false;
      } else {
        str = removeSign;
      }
    }
    int index = 0;
    // remove zeros
    for (int i = 0, n = str.length(); i < n; i++) {
      if (str.charAt(i) != '0') {
        index = i;
        break;
      }
    }
    return NumberUtils.isCreatable(str.substring(index));
  }
}
