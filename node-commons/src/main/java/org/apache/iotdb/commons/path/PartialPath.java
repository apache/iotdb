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
package org.apache.iotdb.commons.path;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.utils.PathUtils;
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.PublicBAOS;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.commons.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

/**
 * A prefix path, suffix path or fullPath generated from SQL. Usually used in the IoTDB server
 * module
 */
public class PartialPath extends Path implements Comparable<Path>, Cloneable {

  private static final Logger logger = LoggerFactory.getLogger(PartialPath.class);

  protected String[] nodes;

  public PartialPath() {}

  /**
   * Construct the PartialPath using a String, will split the given String into String[] E.g., path
   * = "root.sg.`d.1`.`s.1`" nodes = {"root", "sg", "`d.1`", "`s.1`"}
   *
   * @param path a full String of a time series path
   */
  public PartialPath(String path) throws IllegalPathException {
    this.nodes = PathUtils.splitPathToDetachedNodes(path);
    if (nodes.length == 0) {
      throw new IllegalPathException(path);
    }
    // path is root.sg.`abc`, fullPath is root.sg.abc
    // path is root.sg.`select`, fullPath is root.sg.select
    // path is root.sg.`111`, fullPath is root.sg.`111`
    // path is root.sg.`a.b`, fullPath is root.sg.`a.b`
    // path is root.sg.`a``b`, fullPath is root.sg.`a``b`
    this.fullPath = getFullPath();
  }

  public PartialPath(String device, String measurement) throws IllegalPathException {
    String path = device + TsFileConstant.PATH_SEPARATOR + measurement;
    this.nodes = PathUtils.splitPathToDetachedNodes(path);
    this.fullPath = getFullPath();
  }

  /** @param partialNodes nodes of a time series path */
  public PartialPath(String[] partialNodes) {
    nodes = partialNodes;
  }

  /**
   * only use this method in following situations: 1. you are sure you do not want to split the
   * path. 2. you are sure path is correct.
   *
   * @param path path
   * @param needSplit whether to split path to nodes, needSplit can only be false.
   */
  public PartialPath(String path, boolean needSplit) {
    Validate.isTrue(!needSplit);
    fullPath = path;
    if ("".equals(path)) {
      this.nodes = new String[] {};
    } else {
      this.nodes = new String[] {path};
    }
  }

  public boolean hasWildcard() {
    for (String node : nodes) {
      if (ONE_LEVEL_PATH_WILDCARD.equals(node) || MULTI_LEVEL_PATH_WILDCARD.equals(node)) {
        return true;
      }
    }
    return false;
  }

  /**
   * it will return a new partial path
   *
   * @param partialPath the path you want to concat
   * @return new partial path
   */
  public PartialPath concatPath(PartialPath partialPath) {
    int len = nodes.length;
    String[] newNodes = Arrays.copyOf(nodes, nodes.length + partialPath.nodes.length);
    System.arraycopy(partialPath.nodes, 0, newNodes, len, partialPath.nodes.length);
    return new PartialPath(newNodes);
  }

  /**
   * It will change nodes in this partial path
   *
   * @param otherNodes nodes
   */
  public void concatPath(String[] otherNodes) {
    int len = nodes.length;
    this.nodes = Arrays.copyOf(nodes, nodes.length + otherNodes.length);
    System.arraycopy(otherNodes, 0, nodes, len, otherNodes.length);
    fullPath = String.join(TsFileConstant.PATH_SEPARATOR, nodes);
  }

  /**
   * only use this method in following situations: 1. you are sure node is allowed in syntax
   * convention. 2. you are sure node needs not to be checked.
   */
  public PartialPath concatNode(String node) {
    String[] newPathNodes = Arrays.copyOf(nodes, nodes.length + 1);
    newPathNodes[newPathNodes.length - 1] = node;
    return new PartialPath(newPathNodes);
  }

  public String[] getNodes() {
    return nodes;
  }

  public int getNodeLength() {
    return nodes.length;
  }

  public String getTailNode() {
    if (nodes.length <= 0) {
      return "";
    }
    return nodes[nodes.length - 1];
  }

  /**
   * Return the intersection of paths starting with the given prefix and paths matching this path
   * pattern.
   *
   * <p>For example, minimizing "root.**.b.c" with prefix "root.a.b" produces "root.a.b.c",
   * "root.a.b.b.c", "root.a.b.**.b.c", since the multi-level wildcard can match 'a', 'a.b', and any
   * other sub paths start with 'a.b'.
   *
   * <p>The goal of this method is to reduce the search space when querying a database with a path
   * with wildcard.
   *
   * <p>If this path or path pattern doesn't start with given prefix, return empty list. For
   * example, "root.a.b.c".alterPrefixPath("root.b") or "root.a.**".alterPrefixPath("root.b")
   * returns [].
   *
   * @param prefix The prefix. Cannot be null and cannot contain any wildcard.
   */
  public List<PartialPath> alterPrefixPath(PartialPath prefix) {
    Validate.notNull(prefix);

    // Make sure the prefix path doesn't contain any wildcard.
    for (String node : prefix.getNodes()) {
      if (MULTI_LEVEL_PATH_WILDCARD.equals(node) || ONE_LEVEL_PATH_WILDCARD.equals(node)) {
        throw new IllegalArgumentException(
            "Wildcards are not allowed in the prefix path: " + prefix.getFullPath());
      }
    }

    List<List<String>> results = new ArrayList<>();
    alterPrefixPathInternal(
        Arrays.asList(prefix.getNodes()), Arrays.asList(nodes), new ArrayList<>(), results);
    return results.stream()
        .map(r -> new PartialPath(r.toArray(new String[0])))
        .collect(Collectors.toList());
  }

  /**
   * Let PRE stands for the remaining prefix nodes, PATH for the remaining path nodes, and C for the
   * path being processed. And assume the size of the prefix is i and that of this path is j.
   *
   * <p>If the first node of the path is not '**' and it matches the first node of the path. The
   * optimality equation is,
   *
   * <p>O(PRE(0, i), PATH(0, j), C) = O(PRE(1, i), PATH(1, j), [PRE(0)])
   *
   * <p>If the first node of the path is '**', it may match the first 1, 2, ... i nodes of the
   * prefix. '**' may match the all the remaining nodes of the prefix, as well as some additional
   * levels. Thus, the optimality equation is,
   *
   * <p>O(PRE(0, i), PATH(0, j), C) = { O(PRE(1, i), PATH(1, j), [PRE(0)]), ... , O([], PATH(1, j),
   * C + PRE(0) + ... + PRE(i-1)), O([], PATH(0, j), C + PRE(0) + ... + PRE(i-1))}
   *
   * <p>And when all the prefix nodes are matched,
   *
   * <p>O([], PATH, C) = C + PATH
   *
   * <p>The algorithm above takes all the possible matches of a multi-level wildcard into account.
   * Thus, it produces the correct result.
   *
   * <p>To prevent overlapping among the final results, this algorithm stops when the prefix has no
   * remaining node and the first remaining node of the path is '**'. This strategy works because
   * this algorithm will always find the path with the least levels in the search space.
   *
   * @param prefix The remaining nodes of the prefix.
   * @param path The remaining nodes of the path.
   * @param current The path being processed.
   * @param results The final results.
   * @return True if the search should be stopped, else false.
   */
  private boolean alterPrefixPathInternal(
      List<String> prefix, List<String> path, List<String> current, List<List<String>> results) {
    if (prefix.isEmpty()) {
      current.addAll(path);
      results.add(current);
      return !path.isEmpty() && MULTI_LEVEL_PATH_WILDCARD.equals(path.get(0));
    }
    if (path.isEmpty()) {
      return false;
    }

    if (MULTI_LEVEL_PATH_WILDCARD.equals(path.get(0))) {
      // Wildcard matches part of or the whole the prefix.
      for (int j = 1; j <= prefix.size(); j++) {
        List<String> copy = new ArrayList<>(current);
        copy.addAll(prefix.subList(0, j));
        if (alterPrefixPathInternal(
            prefix.subList(j, prefix.size()), path.subList(1, path.size()), copy, results)) {
          return true;
        }
      }
      // Wildcard matches the prefix and some more levels.
      List<String> copy = new ArrayList<>(current);
      copy.addAll(prefix);
      return alterPrefixPathInternal(
          Collections.emptyList(), path.subList(0, path.size()), copy, results);
    } else if (ONE_LEVEL_PATH_WILDCARD.equals(path.get(0)) || prefix.get(0).equals(path.get(0))) {
      // No need to make a copy.
      current.add(prefix.get(0));
      return alterPrefixPathInternal(
          prefix.subList(1, prefix.size()), path.subList(1, path.size()), current, results);
    }

    return false;
  }

  /**
   * Test if current PartialPath matches a full path. Current partialPath acts as a full path
   * pattern. rPath is supposed to be a full timeseries path without wildcards. e.g.
   * "root.sg.device.*" matches path "root.sg.device.s1" whereas it does not match "root.sg.device"
   * and "root.sg.vehicle.s1"
   *
   * @param rPath a plain full path of a timeseries
   * @return true if a successful match, otherwise return false
   */
  public boolean matchFullPath(PartialPath rPath) {
    return matchPath(rPath.getNodes(), 0, 0, false, false);
  }

  /**
   * Check if current pattern PartialPath can match 1 prefix path.
   *
   * <p>1) Current partialPath acts as a full path pattern.
   *
   * <p>2) Input parameter prefixPath is 1 prefix of time-series path.
   *
   * <p>For example:
   *
   * <p>1) Pattern "root.sg1.d1.*" can match prefix path "root.sg1.d1.s1", "root.sg1.d1",
   * "root.sg1", "root" etc.
   *
   * <p>1) Pattern "root.sg1.d1.*" does not match prefix path "root.sg2", "root.sg1.d2".
   *
   * @param prefixPath
   * @return true if a successful match, otherwise return false
   */
  public boolean matchPrefixPath(PartialPath prefixPath) {
    return matchPath(prefixPath.getNodes(), 0, 0, false, true);
  }

  private boolean matchPath(
      String[] pathNodes,
      int pathIndex,
      int patternIndex,
      boolean multiLevelWild,
      boolean pathIsPrefix) {
    if (pathIndex == pathNodes.length && patternIndex == nodes.length) {
      return true;
    } else if (patternIndex == nodes.length && multiLevelWild) {
      return matchPath(pathNodes, pathIndex + 1, patternIndex, true, pathIsPrefix);
    } else if (pathIndex >= pathNodes.length) {
      return pathIsPrefix;
    } else if (patternIndex >= nodes.length) {
      return false;
    }

    String pathNode = pathNodes[pathIndex];
    String patternNode = nodes[patternIndex];
    boolean isMatch = false;
    if (patternNode.equals(MULTI_LEVEL_PATH_WILDCARD)) {
      isMatch = matchPath(pathNodes, pathIndex + 1, patternIndex + 1, true, pathIsPrefix);
    } else {
      if (patternNode.contains(ONE_LEVEL_PATH_WILDCARD)) {
        if (Pattern.matches(patternNode.replace("*", ".*"), pathNode)) {
          isMatch = matchPath(pathNodes, pathIndex + 1, patternIndex + 1, false, pathIsPrefix);
        }
      } else {
        if (patternNode.equals(pathNode)) {
          isMatch = matchPath(pathNodes, pathIndex + 1, patternIndex + 1, false, pathIsPrefix);
        }
      }

      if (!isMatch && multiLevelWild) {
        isMatch = matchPath(pathNodes, pathIndex + 1, patternIndex, true, pathIsPrefix);
      }
    }
    return isMatch;
  }

  /**
   * Test if current PartialPath matches a full path's prefix. Current partialPath acts as a prefix
   * path pattern. rPath is supposed to be a full time-series path without wildcards. e.g. Current
   * PartialPath "root.sg" or "root.*" can match rPath "root.sg.device.s1", "root.sg.device" or
   * "root.sg.vehicle.s1".
   *
   * @param rPath a plain full path of a time-series
   * @return true if a successful match, otherwise return false.
   */
  public boolean prefixMatchFullPath(PartialPath rPath) {
    String[] rNodes = rPath.getNodes();
    if (this.nodes.length > rNodes.length) {
      return false;
    }
    for (int i = 0; i < this.nodes.length; i++) {
      if (nodes[i].equals(MULTI_LEVEL_PATH_WILDCARD)) {
        return true;
      }
      if (nodes[i].equals(ONE_LEVEL_PATH_WILDCARD)) {
        continue;
      }
      if (!nodes[i].equals(rNodes[i])) {
        return false;
      }
    }

    return true;
  }

  /**
   * Test if this path pattern includes input path pattern. e.g. "root.**" includes "root.sg.**",
   * "root.*.d.s" includes "root.sg.d.s", "root.sg.**" does not include "root.**.s", "root.*.d.s"
   * does not include "root.sg.d1.*"
   *
   * @param rPath a pattern path of a timeseries
   * @return true if this path pattern includes input path pattern, otherwise return false
   */
  public boolean include(PartialPath rPath) {
    String[] rNodes = rPath.getNodes();
    String[] lNodes = nodes.clone();
    // Replace * with ** if they are adjacent to each other
    for (int i = 1; i < lNodes.length; i++) {
      if (MULTI_LEVEL_PATH_WILDCARD.equals(lNodes[i - 1])
          && ONE_LEVEL_PATH_WILDCARD.equals(lNodes[i])) {
        lNodes[i] = MULTI_LEVEL_PATH_WILDCARD;
      }
      if (MULTI_LEVEL_PATH_WILDCARD.equals(lNodes[lNodes.length - i])
          && ONE_LEVEL_PATH_WILDCARD.equals(lNodes[lNodes.length - 1 - i])) {
        lNodes[lNodes.length - 1 - i] = MULTI_LEVEL_PATH_WILDCARD;
      }
    }
    // dp[i][j] means if nodes1[0:i) includes nodes[0:j)
    // for example: "root.sg.**" includes "root.sg.d1.*"
    // 1 0 0 0 0 |→| 1 0 0 0 0 |→| 1 0 0 0 0 |→| 1 0 0 0 0
    // 0 0 0 0 0 |↓| 0 1 0 0 0 |→| 0 1 0 0 0 |→| 0 1 0 0 0
    // 0 0 0 0 0 |↓| 0 0 0 0 0 |↓| 0 0 1 0 0 |→| 0 0 1 0 0
    // 0 0 0 0 0 |↓| 0 0 0 0 0 |↓| 0 0 0 0 0 |↓| 0 0 0 1 1
    // Since the derivation of the next row depends only on the previous row, the calculation can
    // be performed using a one-dimensional array
    boolean[] dp = new boolean[rNodes.length + 1];
    dp[0] = true;
    for (int i = 1; i <= lNodes.length; i++) {
      boolean[] newDp = new boolean[rNodes.length + 1];
      for (int j = i; j <= rNodes.length; j++) {
        if (lNodes[i - 1].equals(MULTI_LEVEL_PATH_WILDCARD)) {
          // if encounter MULTI_LEVEL_PATH_WILDCARD
          if (dp[j - 1]) {
            for (int k = j; k <= rNodes.length; k++) {
              newDp[k] = true;
            }
            break;
          }
        } else {
          // if without MULTI_LEVEL_PATH_WILDCARD, scan and check
          if (!rNodes[j - 1].equals(MULTI_LEVEL_PATH_WILDCARD)
              && (lNodes[i - 1].equals(ONE_LEVEL_PATH_WILDCARD)
                  || lNodes[i - 1].equals(rNodes[j - 1]))) {
            // if nodes1[i-1] includes rNodes[j-1], dp[i][j] = dp[i-1][j-1]
            newDp[j] |= dp[j - 1];
          }
        }
      }
      dp = newDp;
    }
    return dp[rNodes.length];
  }

  /**
   * Test if this path pattern overlaps with input path pattern. Overlap means the result sets
   * generated by two path pattern share some common elements. e.g. "root.sg.**" overlaps with
   * "root.**", "root.*.d.s" overlaps with "root.sg.d.s", "root.sg.**" overlaps with "root.**.s",
   * "root.*.d.s" doesn't overlap with "root.sg.d1.*"
   *
   * @param rPath a pattern path of a timeseries
   * @return true if overlapping otherwise return false
   */
  public boolean overlapWith(PartialPath rPath) {
    String[] rNodes = rPath.getNodes();
    for (int i = 0; i < this.nodes.length && i < rNodes.length; i++) {
      // if encounter MULTI_LEVEL_PATH_WILDCARD
      if (nodes[i].equals(MULTI_LEVEL_PATH_WILDCARD)
          || rNodes[i].equals(MULTI_LEVEL_PATH_WILDCARD)) {
        return checkOverlapWithMultiLevelWildcard(nodes, rNodes);
      }
      // if without MULTI_LEVEL_PATH_WILDCARD, scan and check
      if (nodes[i].equals(ONE_LEVEL_PATH_WILDCARD) || rNodes[i].equals(ONE_LEVEL_PATH_WILDCARD)) {
        continue;
      }
      if (!nodes[i].equals(rNodes[i])) {
        return false;
      }
    }
    return this.nodes.length == rNodes.length;
  }

  /**
   * Try to check overlap between nodes1 and nodes2 with MULTI_LEVEL_PATH_WILDCARD. Time complexity
   * O(n^2).
   *
   * @return true if overlapping, otherwise return false
   */
  private boolean checkOverlapWithMultiLevelWildcard(String[] nodes1, String[] nodes2) {
    // dp[i][j] means if nodes1[0:i) and nodes[0:j) overlapping
    boolean[][] dp = new boolean[nodes1.length + 1][nodes2.length + 1];
    dp[0][0] = true;
    for (int i = 1; i <= nodes1.length; i++) {
      for (int j = 1; j <= nodes2.length; j++) {
        if (nodes1[i - 1].equals(MULTI_LEVEL_PATH_WILDCARD)
            || nodes2[j - 1].equals(MULTI_LEVEL_PATH_WILDCARD)) {
          // if encounter MULTI_LEVEL_PATH_WILDCARD
          if (nodes1[i - 1].equals(MULTI_LEVEL_PATH_WILDCARD)) {
            // if nodes1[i-1] is MULTI_LEVEL_PATH_WILDCARD, dp[i][k(k>=j)]=dp[i-1][j-1]
            if (dp[i - 1][j - 1]) {
              for (int k = j; k <= nodes2.length; k++) {
                dp[i][k] = true;
              }
            }
          }
          if (nodes2[j - 1].equals(MULTI_LEVEL_PATH_WILDCARD)) {
            // if nodes2[j-1] is MULTI_LEVEL_PATH_WILDCARD, dp[k(k>=i)][j]=dp[i-1][j-1]
            if (dp[i - 1][j - 1]) {
              for (int k = i; k <= nodes1.length; k++) {
                dp[k][j] = true;
              }
            }
          }
        } else {
          // if without MULTI_LEVEL_PATH_WILDCARD, scan and check
          if (nodes1[i - 1].equals(ONE_LEVEL_PATH_WILDCARD)
              || nodes2[j - 1].equals(ONE_LEVEL_PATH_WILDCARD)
              || nodes1[i - 1].equals(nodes2[j - 1])) {
            // if nodes1[i-1] and nodes[2] is matched, dp[i][j] = dp[i-1][j-1]
            dp[i][j] |= dp[i - 1][j - 1];
          }
        }
      }
    }

    return dp[nodes1.length][nodes2.length];
  }

  @Override
  public String getFullPath() {
    if (fullPath == null) {
      StringBuilder s = new StringBuilder(nodes[0]);
      for (int i = 1; i < nodes.length; i++) {
        s.append(TsFileConstant.PATH_SEPARATOR).append(nodes[i]);
      }
      fullPath = s.toString();
    }
    return fullPath;
  }

  public PartialPath copy() {
    PartialPath result = new PartialPath();
    result.nodes = nodes;
    result.fullPath = fullPath;
    result.device = device;
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof PartialPath)) {
      return false;
    }
    String[] otherNodes = ((PartialPath) obj).getNodes();
    if (this.nodes.length != otherNodes.length) {
      return false;
    } else {
      for (int i = 0; i < this.nodes.length; i++) {
        if (!nodes[i].equals(otherNodes[i])) {
          return false;
        }
      }
    }
    return true;
  }

  @Override
  public boolean equals(String obj) {
    return this.getFullPath().equals(obj);
  }

  @Override
  public int hashCode() {
    int h = 0;
    for (String node : nodes) {
      h += 31 * h + node.hashCode();
    }
    return h;
  }

  @Override
  public String getMeasurement() {
    return nodes[nodes.length - 1];
  }

  public String getFirstNode() {
    return nodes[0];
  }

  @Override
  public String getDevice() {
    if (device != null) {
      return device;
    } else {
      if (nodes.length == 1) {
        return "";
      }
      StringBuilder s = new StringBuilder(nodes[0]);
      for (int i = 1; i < nodes.length - 1; i++) {
        s.append(TsFileConstant.PATH_SEPARATOR);
        s.append(nodes[i]);
      }
      device = s.toString();
    }
    return device;
  }

  // todo remove measurement related interface after invoker using MeasurementPath explicitly
  public String getMeasurementAlias() {
    throw new RuntimeException("Only MeasurementPath support alias");
  }

  public void setMeasurementAlias(String measurementAlias) {
    throw new RuntimeException("Only MeasurementPath support alias");
  }

  public boolean isMeasurementAliasExists() {
    return false;
  }

  @Override
  public String getFullPathWithAlias() {
    throw new RuntimeException("Only MeasurementPath support alias");
  }

  public IMeasurementSchema getMeasurementSchema() throws MetadataException {
    throw new MetadataException("This path doesn't represent a measurement");
  }

  public TSDataType getSeriesType() throws UnsupportedOperationException {
    throw new UnsupportedOperationException("This path doesn't represent a measurement");
  }

  @Override
  public int compareTo(Path path) {
    PartialPath partialPath = (PartialPath) path;
    return this.getFullPath().compareTo(partialPath.getFullPath());
  }

  public boolean startsWith(String[] otherNodes) {
    for (int i = 0; i < otherNodes.length; i++) {
      if (!nodes[i].equals(otherNodes[i])) {
        return false;
      }
    }
    return true;
  }

  public boolean startWith(String otherNode) {
    return nodes[0].equals(otherNode);
  }

  public boolean containNode(String otherNode) {
    for (String node : nodes) {
      if (node.equals(otherNode)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public String toString() {
    return getFullPath();
  }

  public PartialPath getDevicePath() {
    return new PartialPath(Arrays.copyOf(nodes, nodes.length - 1));
  }

  public List<PartialPath> getDevicePathPattern() {
    List<PartialPath> result = new ArrayList<>();
    result.add(getDevicePath());
    if (nodes[nodes.length - 1].equals(MULTI_LEVEL_PATH_WILDCARD)) {
      result.add(new PartialPath(nodes));
    }
    return result;
  }

  @TestOnly
  public Path toTSFilePath() {
    return new Path(getDevice(), getMeasurement(), true);
  }

  public static List<String> toStringList(List<PartialPath> pathList) {
    List<String> ret = new ArrayList<>();
    for (PartialPath path : pathList) {
      ret.add(path.getFullPath());
    }
    return ret;
  }

  /**
   * Convert a list of Strings to a list of PartialPaths, ignoring all illegal paths
   *
   * @param pathList
   * @return
   */
  public static List<PartialPath> fromStringList(List<String> pathList) {
    if (pathList == null || pathList.isEmpty()) {
      return Collections.emptyList();
    }

    List<PartialPath> ret = new ArrayList<>();
    for (String s : pathList) {
      try {
        ret.add(new PartialPath(s));
      } catch (IllegalPathException e) {
        logger.warn("Encountered an illegal path {}", s);
      }
    }
    return ret;
  }

  @Override
  public PartialPath clone() {
    return new PartialPath(this.getNodes().clone());
  }

  public ByteBuffer serialize() throws IOException {
    PublicBAOS publicBAOS = new PublicBAOS();
    serialize((OutputStream) publicBAOS);
    return ByteBuffer.wrap(publicBAOS.getBuf(), 0, publicBAOS.size());
  }

  @Override
  public void serialize(OutputStream stream) throws IOException {
    PathType.Partial.serialize(stream);
    serializeWithoutType(stream);
  }

  @Override
  public void serialize(ByteBuffer byteBuffer) {
    PathType.Partial.serialize(byteBuffer);
    serializeWithoutType(byteBuffer);
  }

  @Override
  public void serialize(PublicBAOS stream) throws IOException {
    PathType.Partial.serialize(stream);
    serializeWithoutType(stream);
  }

  @Override
  protected void serializeWithoutType(ByteBuffer byteBuffer) {
    super.serializeWithoutType(byteBuffer);
    ReadWriteIOUtils.write(nodes.length, byteBuffer);
    for (String node : nodes) {
      ReadWriteIOUtils.write(node, byteBuffer);
    }
  }

  @Override
  protected void serializeWithoutType(OutputStream stream) throws IOException {
    super.serializeWithoutType(stream);
    ReadWriteIOUtils.write(nodes.length, stream);
    for (String node : nodes) {
      ReadWriteIOUtils.write(node, stream);
    }
  }

  // Attention!!! If you want to use serialize and deserialize of partialPath, must invoke
  // PathDeserializeUtil.deserialize
  public static PartialPath deserialize(ByteBuffer byteBuffer) {
    Path path = Path.deserialize(byteBuffer);
    PartialPath partialPath = new PartialPath();
    int nodeSize = ReadWriteIOUtils.readInt(byteBuffer);
    String[] nodes = new String[nodeSize];
    for (int i = 0; i < nodeSize; i++) {
      nodes[i] = ReadWriteIOUtils.readString(byteBuffer);
    }
    partialPath.nodes = nodes;
    partialPath.setMeasurement(path.getMeasurement());
    partialPath.device = path.getDevice();
    partialPath.fullPath = path.getFullPath();
    return partialPath;
  }

  public PartialPath transformToPartialPath() {
    return this;
  }

  /**
   * PartialPath basic total, 52B
   *
   * <ul>
   *   <li>Object header, 8B
   *   <li>String[] reference + header + length, 8 + 4 + 8= 20B
   *   <li>Path attributes' references, 8 * 3 = 24B
   * </ul>
   */
  public static int estimateSize(PartialPath partialPath) {
    int size = 52;
    for (String node : partialPath.getNodes()) {
      size += estimateStringSize(node);
    }
    size += estimateStringSize(partialPath.getFullPath());
    return size;
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
  private static int estimateStringSize(String string) {
    // each char takes 2B in Java
    return string == null ? 0 : 32 + 2 * string.length();
  }
}
