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
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

abstract class TreeByMetadataVirtualDirectoryResolver implements VirtualDirectoryResolver {

  private static final String SHOW_ALL_TIMESERIES = "SHOW TIMESERIES root.**";

  private final String name;
  private final String metadataColumn;
  private final String description;
  private final String keyKind;
  private final String valueKind;
  private final FilesystemSchemaProvider delegate;
  private final SqlExecutor executor;

  TreeByMetadataVirtualDirectoryResolver(
      String name,
      String metadataColumn,
      String description,
      String keyKind,
      String valueKind,
      SqlExecutor executor,
      FilesystemSchemaProvider delegate) {
    this.name = name;
    this.metadataColumn = metadataColumn;
    this.description = description;
    this.keyKind = keyKind;
    this.valueKind = valueKind;
    this.executor = executor;
    this.delegate = delegate;
  }

  @Override
  public String name() {
    return name;
  }

  @Override
  public FsNode rootNode() {
    return VirtualDirectoryNodes.root(name, description);
  }

  @Override
  public List<FsNode> list(FsPath path) throws SQLException {
    List<String> segments = VirtualDirectoryPaths.resolverSegments(path);
    if (segments.isEmpty()) {
      return listKeyDirectories();
    }
    if (segments.size() == 1) {
      return listValueDirectories(path, VirtualDirectorySegments.decode(segments.get(0)));
    }
    if (segments.size() == 2) {
      return listTimeseriesFiles(
          path,
          VirtualDirectorySegments.decode(segments.get(0)),
          VirtualDirectorySegments.decode(segments.get(1)));
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
      String key = VirtualDirectorySegments.decode(segments.get(0));
      return VirtualDirectoryNodes.directory(
          path.getFileName(),
          path,
          name,
          "",
          keyKind,
          TreeTimeseriesSupport.displayMetadata("metadataKey", key));
    }
    if (segments.size() == 2) {
      String value = VirtualDirectorySegments.decode(segments.get(1));
      return VirtualDirectoryNodes.directory(
          path.getFileName(),
          path,
          name,
          "",
          valueKind,
          TreeTimeseriesSupport.displayMetadata("metadataValue", value));
    }
    if (segments.size() == 3) {
      String timeseries = VirtualDirectorySegments.decode(segments.get(2));
      return VirtualDirectoryNodes.rewrite(
          delegate.describe(TreeTimeseriesSupport.canonicalTimeseriesPath(timeseries)),
          path.getFileName(),
          path,
          name,
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

  private List<FsNode> listKeyDirectories() throws SQLException {
    Set<String> keys = new LinkedHashSet<>();
    for (SqlRow row : TreeTimeseriesSupport.query(executor, SHOW_ALL_TIMESERIES)) {
      keys.addAll(TreeTimeseriesSupport.parseMetadataMap(row, metadataColumn).keySet());
    }
    List<FsNode> nodes = new ArrayList<>();
    for (String key : keys) {
      String encodedKey = VirtualDirectorySegments.encode(key);
      nodes.add(
          VirtualDirectoryNodes.directory(
              encodedKey,
              VirtualDirectoryPaths.resolverRootPath(name).resolve(encodedKey),
              name,
              "",
              keyKind,
              TreeTimeseriesSupport.displayMetadata("metadataKey", key)));
    }
    return nodes;
  }

  private List<FsNode> listValueDirectories(FsPath path, String key) throws SQLException {
    Set<String> values = new LinkedHashSet<>();
    for (SqlRow row : TreeTimeseriesSupport.query(executor, SHOW_ALL_TIMESERIES)) {
      String value = TreeTimeseriesSupport.parseMetadataMap(row, metadataColumn).get(key);
      if (value != null) {
        values.add(value);
      }
    }
    List<FsNode> nodes = new ArrayList<>();
    for (String value : values) {
      String encodedValue = VirtualDirectorySegments.encode(value);
      nodes.add(
          VirtualDirectoryNodes.directory(
              encodedValue,
              path.resolve(encodedValue),
              name,
              "",
              valueKind,
              TreeTimeseriesSupport.displayMetadata("metadataValue", value)));
    }
    return nodes;
  }

  private List<FsNode> listTimeseriesFiles(FsPath path, String key, String value)
      throws SQLException {
    List<FsNode> nodes = new ArrayList<>();
    for (SqlRow row :
        TreeTimeseriesSupport.query(
            executor,
            SHOW_ALL_TIMESERIES
                + " WHERE "
                + TreeTimeseriesSupport.quote(key)
                + " = "
                + TreeTimeseriesSupport.quote(value))) {
      String timeseries = TreeTimeseriesSupport.timeseriesPath(row);
      if (timeseries == null) {
        continue;
      }
      String encodedTimeseries = VirtualDirectorySegments.encode(timeseries);
      Map<String, String> metadata =
          TreeTimeseriesSupport.displayMetadata("timeseries", timeseries);
      nodes.add(
          VirtualDirectoryNodes.rewrite(
              TreeTimeseriesSupport.timeseriesNode(row),
              encodedTimeseries,
              path.resolve(encodedTimeseries),
              name,
              metadata));
    }
    return nodes;
  }

  private FsPath canonicalLeafPath(FsPath path) throws SQLException {
    List<String> segments = VirtualDirectoryPaths.resolverSegments(path);
    if (segments.size() != 3) {
      throw new SQLException("Path is not readable: " + path);
    }
    return TreeTimeseriesSupport.canonicalTimeseriesPath(
        VirtualDirectorySegments.decode(segments.get(2)));
  }
}
