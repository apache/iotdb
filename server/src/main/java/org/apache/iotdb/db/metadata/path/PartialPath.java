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
package org.apache.iotdb.db.metadata.path;

import org.apache.iotdb.db.engine.memtable.IMemTable;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.querycontext.QueryDataSource;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.query.QueryProcessException;
import org.apache.iotdb.db.metadata.utils.MetaUtils;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.executor.fill.LastPointReader;
import org.apache.iotdb.db.query.filter.TsFileFilter;
import org.apache.iotdb.db.query.reader.series.SeriesReader;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.common.constant.TsFileConstant;
import org.apache.iotdb.tsfile.file.metadata.IChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.ITimeSeriesMetadata;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.filter.basic.Filter;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import static org.apache.iotdb.db.conf.IoTDBConstant.MULTI_LEVEL_PATH_WILDCARD;
import static org.apache.iotdb.db.conf.IoTDBConstant.ONE_LEVEL_PATH_WILDCARD;

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
   * = "root.sg.\"d.1\".\"s.1\"" nodes = {"root", "sg", "\"d.1\"", "\"s.1\""}
   *
   * @param path a full String of a time series path
   * @throws IllegalPathException
   */
  public PartialPath(String path) throws IllegalPathException {
    this.nodes = MetaUtils.splitPathToDetachedPath(path);
    this.fullPath = path;
  }

  public PartialPath(String device, String measurement) throws IllegalPathException {
    this.fullPath = device + TsFileConstant.PATH_SEPARATOR + measurement;
    this.nodes = MetaUtils.splitPathToDetachedPath(fullPath);
  }

  /** @param partialNodes nodes of a time series path */
  public PartialPath(String[] partialNodes) {
    nodes = partialNodes;
  }

  /**
   * @param path path
   * @param needSplit needSplit is basically false, whether need to be split to device and
   *     measurement, doesn't support escape character yet.
   */
  public PartialPath(String path, boolean needSplit) {
    super(path, needSplit);
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
   * <p>The goal of this method is to reduce the search space when querying a storage group with a
   * path with wildcard.
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
   * Test if this PartialPath matches a full path. This partialPath acts as a full path pattern.
   * rPath is supposed to be a full timeseries path without wildcards. e.g. "root.sg.device.*"
   * matches path "root.sg.device.s1" whereas it does not match "root.sg.device" and
   * "root.sg.vehicle.s1"
   *
   * @param rPath a plain full path of a timeseries
   * @return true if a successful match, otherwise return false
   */
  public boolean matchFullPath(PartialPath rPath) {
    return matchFullPath(rPath.getNodes(), 0, 0, false);
  }

  private boolean matchFullPath(
      String[] pathNodes, int pathIndex, int patternIndex, boolean multiLevelWild) {
    if (pathIndex == pathNodes.length && patternIndex == nodes.length) {
      return true;
    } else if (patternIndex == nodes.length && multiLevelWild) {
      return matchFullPath(pathNodes, pathIndex + 1, patternIndex, true);
    } else if (pathIndex >= pathNodes.length || patternIndex >= nodes.length) {
      return false;
    }

    String pathNode = pathNodes[pathIndex];
    String patternNode = nodes[patternIndex];
    boolean isMatch = false;
    if (patternNode.equals(MULTI_LEVEL_PATH_WILDCARD)) {
      isMatch = matchFullPath(pathNodes, pathIndex + 1, patternIndex + 1, true);
    } else {
      if (patternNode.contains(ONE_LEVEL_PATH_WILDCARD)) {
        if (Pattern.matches(patternNode.replace("*", ".*"), pathNode)) {
          isMatch = matchFullPath(pathNodes, pathIndex + 1, patternIndex + 1, false);
        }
      } else {
        if (patternNode.equals(pathNode)) {
          isMatch = matchFullPath(pathNodes, pathIndex + 1, patternIndex + 1, false);
        }
      }

      if (!isMatch && multiLevelWild) {
        isMatch = matchFullPath(pathNodes, pathIndex + 1, patternIndex, true);
      }
    }
    return isMatch;
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
    return this.getFullPath().hashCode();
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
      return device;
    }
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

  @Override
  public String toString() {
    return getFullPath();
  }

  public PartialPath getDevicePath() {
    return new PartialPath(Arrays.copyOf(nodes, nodes.length - 1));
  }

  @TestOnly
  public Path toTSFilePath() {
    return new Path(getDevice(), getMeasurement());
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

  public LastPointReader createLastPointReader(
      TSDataType dataType,
      Set<String> deviceMeasurements,
      QueryContext context,
      QueryDataSource dataSource,
      long queryTime,
      Filter timeFilter) {
    throw new UnsupportedOperationException("Should call exact sub class!");
  }

  public SeriesReader createSeriesReader(
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      QueryDataSource dataSource,
      Filter timeFilter,
      Filter valueFilter,
      TsFileFilter fileFilter,
      boolean ascending) {
    throw new UnsupportedOperationException("Should call exact sub class!");
  }

  @TestOnly
  public SeriesReader createSeriesReader(
      Set<String> allSensors,
      TSDataType dataType,
      QueryContext context,
      List<TsFileResource> seqFileResource,
      List<TsFileResource> unseqFileResource,
      Filter timeFilter,
      Filter valueFilter,
      boolean ascending) {
    throw new UnsupportedOperationException("Should call exact sub class!");
  }

  public TsFileResource createTsFileResource(
      List<ReadOnlyMemChunk> readOnlyMemChunk,
      List<IChunkMetadata> chunkMetadataList,
      TsFileResource originTsFileResource)
      throws IOException {
    throw new UnsupportedOperationException("Should call exact sub class!");
  }

  public ITimeSeriesMetadata generateTimeSeriesMetadata(
      List<ReadOnlyMemChunk> readOnlyMemChunk, List<IChunkMetadata> chunkMetadataList)
      throws IOException {
    throw new UnsupportedOperationException("Should call exact sub class!");
  }

  /**
   * get the ReadOnlyMemChunk from the given MemTable.
   *
   * @return ReadOnlyMemChunk
   */
  public ReadOnlyMemChunk getReadOnlyMemChunkFromMemTable(
      IMemTable memTable, List<Pair<Modification, IMemTable>> modsToMemtable, long timeLowerBound)
      throws QueryProcessException, IOException {
    throw new UnsupportedOperationException("Should call exact sub class!");
  }

  /** get modifications from a memtable. */
  protected List<Modification> getModificationsForMemtable(
      IMemTable memTable, List<Pair<Modification, IMemTable>> modsToMemtable) {
    List<Modification> modifications = new ArrayList<>();
    boolean foundMemtable = false;
    for (Pair<Modification, IMemTable> entry : modsToMemtable) {
      if (foundMemtable || entry.right.equals(memTable)) {
        modifications.add(entry.left);
        foundMemtable = true;
      }
    }
    return modifications;
  }

  @Override
  public PartialPath clone() {
    return new PartialPath(this.getNodes().clone());
  }

  public List<IChunkMetadata> getVisibleMetadataListFromWriter(
      RestorableTsFileIOWriter writer, TsFileResource tsFileResource, QueryContext context) {
    throw new UnsupportedOperationException("Should call exact sub class!");
  }
}
