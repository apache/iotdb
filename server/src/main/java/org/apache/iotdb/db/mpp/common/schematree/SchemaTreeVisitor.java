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

package org.apache.iotdb.db.mpp.common.schematree;

import org.apache.iotdb.db.metadata.path.MeasurementPath;
import org.apache.iotdb.db.metadata.path.PartialPath;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.regex.Pattern;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

public class SchemaTreeVisitor implements Iterator<MeasurementPath> {

  private final SchemaNode root;
  private final String[] nodes;
  private final boolean isPrefixMatch;

  private final int limit;
  private final int offset;
  private final boolean hasLimit;

  private int count = 0;
  private int curOffset = -1;

  private final Deque<Integer> indexStack = new ArrayDeque<>();
  private final Deque<Iterator<SchemaNode>> stack = new ArrayDeque<>();
  private final Deque<SchemaNode> context = new ArrayDeque<>();

  private SchemaMeasurementNode nextMatchedNode;

  public SchemaTreeVisitor(
      SchemaNode root, PartialPath pathPattern, int slimit, int soffset, boolean isPrefixMatch) {
    this.root = root;
    nodes = pathPattern.getNodes();
    this.isPrefixMatch = isPrefixMatch;

    limit = slimit;
    offset = soffset;
    hasLimit = slimit != 0;

    indexStack.push(0);
    stack.push(Collections.singletonList(root).iterator());
  }

  @Override
  public boolean hasNext() {
    if (nextMatchedNode == null) {
      getNext();
    }
    return nextMatchedNode != null;
  }

  @Override
  public MeasurementPath next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    MeasurementPath result = generateMeasurementPath();
    nextMatchedNode = null;
    return result;
  }

  public List<MeasurementPath> getAllResult() {
    List<MeasurementPath> result = new ArrayList<>();
    while (hasNext()) {
      result.add(next());
    }
    return result;
  }

  public int getNextOffset() {
    return curOffset + 1;
  }

  public void resetStatus() {
    count = 0;
    curOffset = -1;
    context.clear();
    indexStack.clear();
    indexStack.push(0);
    stack.clear();
    stack.push(Collections.singletonList(root).iterator());
  }

  private void getNext() {
    if (hasLimit && count == limit) {
      return;
    }

    int patternIndex;
    SchemaNode node;
    Iterator<SchemaNode> iterator;
    while (!stack.isEmpty()) {
      iterator = stack.peek();

      if (!iterator.hasNext()) {
        popStack();
        continue;
      }

      node = iterator.next();
      patternIndex = indexStack.peek();
      if (patternIndex >= nodes.length - 1) {
        if (node.isMeasurement()) {
          if (hasLimit) {
            curOffset += 1;
            if (curOffset < offset) {
              continue;
            }
            count++;
          }

          nextMatchedNode = node.getAsMeasurementNode();
          return;
        }

        if (nodes[nodes.length - 1].equals(MULTI_LEVEL_PATH_WILDCARD) || isPrefixMatch) {
          pushAllChildren(node, patternIndex);
        }

        continue;
      }

      if (nodes[patternIndex].equals(ONE_LEVEL_PATH_WILDCARD)) {
        String regex = nodes[patternIndex].replace("*", ".*");
        while (!checkOneLevelWildcardMatch(regex, node) && iterator.hasNext()) {
          node = iterator.next();
        }
        if (!checkOneLevelWildcardMatch(regex, node)) {
          popStack();
          continue;
        }
      }

      if (node.isMeasurement()) {
        continue;
      }

      if (nodes[patternIndex + 1].contains(ONE_LEVEL_PATH_WILDCARD)) {
        pushAllChildren(node, patternIndex + 1);
      } else {
        pushSingleChild(node, nodes[patternIndex + 1], patternIndex + 1);
      }
    }
  }

  private void popStack() {
    stack.pop();
    int patternIndex = indexStack.pop();
    if (patternIndex == 0) {
      return;
    }
    SchemaNode node = context.pop();

    if (indexStack.isEmpty()) {
      return;
    }

    int parentIndex = indexStack.peek();
    if (patternIndex != parentIndex
        && parentIndex < nodes.length - 1
        && nodes[parentIndex].equals(MULTI_LEVEL_PATH_WILDCARD)) {
      pushAllChildren(node, parentIndex);
    }
  }

  private void pushAllChildren(SchemaNode node, int patternIndex) {
    stack.push(node.getChildrenIterator());
    context.push(node);
    indexStack.push(patternIndex);
  }

  private void pushSingleChild(SchemaNode node, String childName, int patternIndex) {
    SchemaNode child = node.getChild(childName);
    if (child != null) {
      stack.push(Collections.singletonList(child).iterator());
      context.push(node);
      indexStack.push(patternIndex);
    }
  }

  private boolean checkOneLevelWildcardMatch(String regex, SchemaNode node) {
    if (!node.isMeasurement()) {
      return Pattern.matches(regex, node.getName());
    }

    SchemaMeasurementNode measurementNode = node.getAsMeasurementNode();

    return Pattern.matches(regex, measurementNode.getName())
        || Pattern.matches(regex, measurementNode.getAlias());
  }

  private MeasurementPath generateMeasurementPath() {
    List<String> nodeNames = new ArrayList<>();
    Iterator<SchemaNode> iterator = context.descendingIterator();
    while (iterator.hasNext()) {
      nodeNames.add(iterator.next().getName());
    }
    nodeNames.add(nextMatchedNode.getName());
    MeasurementPath result =
        new MeasurementPath(nodeNames.toArray(new String[0]), nextMatchedNode.getSchema());
    result.setUnderAlignedEntity(context.peek().getAsEntityNode().isAligned());
    String alias = nextMatchedNode.getAlias();
    if (nodes[nodes.length - 1].equals(alias)) {
      result.setMeasurementAlias(alias);
    }

    return result;
  }
}
