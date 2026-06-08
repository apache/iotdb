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

package org.apache.iotdb.cli.fs.virtualdir;

import org.apache.iotdb.cli.fs.node.FsNode;
import org.apache.iotdb.cli.fs.path.FsPath;
import org.apache.iotdb.cli.fs.provider.FilesystemSchemaProvider;
import org.apache.iotdb.cli.fs.sql.SqlExecutor;
import org.apache.iotdb.cli.fs.sql.SqlRow;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class TreeByMeasurementVirtualDirectoryResolver implements VirtualDirectoryResolver {

  static final String NAME = "by-measurement";

  private final SqlExecutor executor;
  private final FilesystemSchemaProvider delegate;

  public TreeByMeasurementVirtualDirectoryResolver(
      SqlExecutor executor, FilesystemSchemaProvider delegate) {
    this.executor = executor;
    this.delegate = delegate;
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public FsNode rootNode() {
    return VirtualDirectoryNodes.root(NAME, "Browse tree timeseries grouped by measurement name");
  }

  @Override
  public List<FsNode> list(FsPath path) throws SQLException {
    List<String> segments = VirtualDirectoryPaths.resolverSegments(path);
    if (segments.isEmpty()) {
      return listMeasurementDirectories();
    }
    if (segments.size() == 1) {
      return listTimeseriesFiles(
          path, "SHOW TIMESERIES root.**." + VirtualDirectorySegments.decode(segments.get(0)));
    }
    return new ArrayList<>();
  }

  @Override
  public FsNode describe(FsPath path) throws SQLException {
    List<String> segments = VirtualDirectoryPaths.resolverSegments(path);
    if (segments.isEmpty()) {
      return rootNode();
    }
    if (segments.size() == 1) {
      String measurement = VirtualDirectorySegments.decode(segments.get(0));
      return VirtualDirectoryNodes.directory(
          path.getFileName(),
          path,
          NAME,
          "",
          "measurement",
          TreeTimeseriesSupport.displayMetadata("measurement", measurement));
    }
    if (segments.size() == 2) {
      String timeseries = VirtualDirectorySegments.decode(segments.get(1));
      return VirtualDirectoryNodes.rewrite(
          delegate.describe(TreeTimeseriesSupport.canonicalTimeseriesPath(timeseries)),
          path.getFileName(),
          path,
          NAME,
          TreeTimeseriesSupport.displayMetadata("timeseries", timeseries));
    }
    return VirtualDirectoryPaths.unknown(path);
  }

  @Override
  public List<SqlRow> read(FsPath path, int limit) throws SQLException {
    return delegate.read(canonicalLeafPath(path), limit);
  }

  @Override
  public List<SqlRow> tail(FsPath path, int limit) throws SQLException {
    return delegate.tail(canonicalLeafPath(path), limit);
  }

  @Override
  public long count(FsPath path) throws SQLException {
    return delegate.count(canonicalLeafPath(path));
  }

  private List<FsNode> listMeasurementDirectories() throws SQLException {
    Set<String> measurements = new LinkedHashSet<>();
    for (SqlRow row : TreeTimeseriesSupport.query(executor, "SHOW TIMESERIES root.**")) {
      String timeseries = TreeTimeseriesSupport.timeseriesPath(row);
      if (timeseries != null) {
        measurements.add(TreeTimeseriesSupport.measurementName(timeseries));
      }
    }
    List<FsNode> nodes = new ArrayList<>();
    for (String measurement : measurements) {
      String encodedMeasurement = VirtualDirectorySegments.encode(measurement);
      nodes.add(
          VirtualDirectoryNodes.directory(
              encodedMeasurement,
              VirtualDirectoryPaths.resolverRootPath(NAME).resolve(encodedMeasurement),
              NAME,
              "",
              "measurement",
              TreeTimeseriesSupport.displayMetadata("measurement", measurement)));
    }
    return nodes;
  }

  private List<FsNode> listTimeseriesFiles(FsPath path, String sql) throws SQLException {
    List<FsNode> nodes = new ArrayList<>();
    for (SqlRow row : TreeTimeseriesSupport.query(executor, sql)) {
      String timeseries = TreeTimeseriesSupport.timeseriesPath(row);
      if (timeseries == null) {
        continue;
      }
      String encodedTimeseries = VirtualDirectorySegments.encode(timeseries);
      Map<String, String> metadata = new LinkedHashMap<>();
      metadata.put("timeseries", timeseries);
      nodes.add(
          VirtualDirectoryNodes.rewrite(
              TreeTimeseriesSupport.timeseriesNode(row),
              encodedTimeseries,
              path.resolve(encodedTimeseries),
              NAME,
              metadata));
    }
    return nodes;
  }

  private FsPath canonicalLeafPath(FsPath path) throws SQLException {
    List<String> segments = VirtualDirectoryPaths.resolverSegments(path);
    if (segments.size() != 2) {
      throw new SQLException("Path is not readable: " + path);
    }
    return TreeTimeseriesSupport.canonicalTimeseriesPath(
        VirtualDirectorySegments.decode(segments.get(1)));
  }
}
