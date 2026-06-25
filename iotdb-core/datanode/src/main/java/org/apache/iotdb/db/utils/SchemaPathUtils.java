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

package org.apache.iotdb.db.utils;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternUtil;

import org.apache.tsfile.common.constant.TsFileConstant;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Pattern;

public final class SchemaPathUtils {

  private static final Pattern SQL_NUMERIC_NODE_PATTERN = Pattern.compile("\\d+([eE][+-]?\\d+)?");

  private SchemaPathUtils() {}

  public static String getFullPathWithNecessaryBackQuotes(final PartialPath path) {
    return getFullPathWithNecessaryBackQuotes(path.getNodes());
  }

  public static String getFullPathWithNecessaryBackQuotes(final String path) {
    try {
      return getFullPathWithNecessaryBackQuotes(new PartialPath(path));
    } catch (IllegalPathException e) {
      return getFullPathWithNecessaryBackQuotes(splitPathPreservingBackQuotes(path));
    }
  }

  public static String getPathPatternWithNecessaryBackQuotes(final PartialPath pathPattern) {
    return getPathPatternWithNecessaryBackQuotes(pathPattern.getNodes());
  }

  public static String getPathPatternWithNecessaryBackQuotes(final String pathPattern) {
    try {
      return getPathPatternWithNecessaryBackQuotes(new PartialPath(pathPattern));
    } catch (IllegalPathException e) {
      return getPathPatternWithNecessaryBackQuotes(splitPathPreservingBackQuotes(pathPattern));
    }
  }

  public static String getTailNodeWithNecessaryBackQuotes(final String path) {
    try {
      return getNodeWithNecessaryBackQuotes(new PartialPath(path).getTailNode());
    } catch (IllegalPathException e) {
      final String[] nodes = splitPathPreservingBackQuotes(path);
      return nodes.length == 0 ? "" : getNodeWithNecessaryBackQuotes(nodes[nodes.length - 1]);
    }
  }

  public static String getNodeWithNecessaryBackQuotes(final String node) {
    return getNodeWithNecessaryBackQuotes(node, false);
  }

  private static String getFullPathWithNecessaryBackQuotes(final String[] nodes) {
    if (nodes.length == 0) {
      return "";
    }
    final StringBuilder builder = new StringBuilder(getNodeWithNecessaryBackQuotes(nodes[0], true));
    for (int i = 1; i < nodes.length; i++) {
      builder
          .append(TsFileConstant.PATH_SEPARATOR)
          .append(getNodeWithNecessaryBackQuotes(nodes[i], false));
    }
    return builder.toString();
  }

  private static String getPathPatternWithNecessaryBackQuotes(final String[] nodes) {
    if (nodes.length == 0) {
      return "";
    }
    final StringBuilder builder =
        new StringBuilder(getPathPatternNodeWithNecessaryBackQuotes(nodes[0], true));
    for (int i = 1; i < nodes.length; i++) {
      builder
          .append(TsFileConstant.PATH_SEPARATOR)
          .append(getPathPatternNodeWithNecessaryBackQuotes(nodes[i], false));
    }
    return builder.toString();
  }

  private static String[] splitPathPreservingBackQuotes(final String path) {
    final List<String> nodes = new ArrayList<>();
    final StringBuilder currentNode = new StringBuilder();
    boolean inBackQuotes = false;
    for (int i = 0; i < path.length(); i++) {
      final char currentChar = path.charAt(i);
      if (currentChar == TsFileConstant.BACK_QUOTE_STRING.charAt(0)) {
        currentNode.append(currentChar);
        if (inBackQuotes
            && i + 1 < path.length()
            && path.charAt(i + 1) == TsFileConstant.BACK_QUOTE_STRING.charAt(0)) {
          currentNode.append(path.charAt(++i));
        } else {
          inBackQuotes = !inBackQuotes;
        }
      } else if (currentChar == TsFileConstant.PATH_SEPARATOR.charAt(0) && !inBackQuotes) {
        nodes.add(currentNode.toString());
        currentNode.setLength(0);
      } else {
        currentNode.append(currentChar);
      }
    }
    nodes.add(currentNode.toString());
    return nodes.toArray(new String[0]);
  }

  private static String getNodeWithNecessaryBackQuotes(final String node, final boolean isFirst) {
    if (node == null
        || (isFirst && IoTDBConstant.PATH_ROOT.equalsIgnoreCase(node))
        || (node.startsWith(TsFileConstant.BACK_QUOTE_STRING)
            && node.endsWith(TsFileConstant.BACK_QUOTE_STRING))) {
      return node;
    }
    if (IoTDBConstant.reservedWords.contains(node.toUpperCase(Locale.ENGLISH))
        || isSqlNumericNode(node)
        || !TsFileConstant.IDENTIFIER_PATTERN.matcher(node).matches()) {
      return TsFileConstant.BACK_QUOTE_STRING
          + node.replace(TsFileConstant.BACK_QUOTE_STRING, TsFileConstant.DOUBLE_BACK_QUOTE_STRING)
          + TsFileConstant.BACK_QUOTE_STRING;
    }
    return node;
  }

  private static String getPathPatternNodeWithNecessaryBackQuotes(
      final String node, final boolean isFirst) {
    if (node == null || PathPatternUtil.hasWildcard(node)) {
      return node;
    }
    return getNodeWithNecessaryBackQuotes(node, isFirst);
  }

  private static boolean isSqlNumericNode(final String node) {
    return SQL_NUMERIC_NODE_PATTERN.matcher(node).matches();
  }
}
