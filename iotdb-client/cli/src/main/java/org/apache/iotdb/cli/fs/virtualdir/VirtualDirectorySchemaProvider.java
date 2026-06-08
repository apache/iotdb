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
import org.apache.iotdb.cli.fs.sql.SqlRow;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class VirtualDirectorySchemaProvider implements FilesystemSchemaProvider {

  private final FilesystemSchemaProvider delegate;
  private final Map<String, VirtualDirectoryResolver> resolvers = new LinkedHashMap<>();

  public VirtualDirectorySchemaProvider(
      FilesystemSchemaProvider delegate, List<VirtualDirectoryResolver> resolvers) {
    this.delegate = delegate;
    for (VirtualDirectoryResolver resolver : resolvers) {
      this.resolvers.put(resolver.name(), resolver);
    }
  }

  @Override
  public List<FsNode> list(FsPath path) throws SQLException {
    if (path.isRoot()) {
      return listRoot(path);
    }
    if (VirtualDirectoryPaths.isVirtualRoot(path)) {
      return listVirtualRoots();
    }
    if (VirtualDirectoryPaths.isVirtualPath(path)) {
      VirtualDirectoryResolver resolver = resolver(path);
      return resolver == null ? new ArrayList<>() : resolver.list(path);
    }
    return delegate.list(path);
  }

  @Override
  public FsNode describe(FsPath path) throws SQLException {
    if (VirtualDirectoryPaths.isVirtualRoot(path)) {
      return VirtualDirectoryPaths.virtualRootNode();
    }
    if (VirtualDirectoryPaths.isVirtualPath(path)) {
      VirtualDirectoryResolver resolver = resolver(path);
      return resolver == null ? VirtualDirectoryPaths.unknown(path) : resolver.describe(path);
    }
    return delegate.describe(path);
  }

  @Override
  public List<SqlRow> read(FsPath path, int limit) throws SQLException {
    VirtualDirectoryResolver resolver = readableResolver(path, "Path is not readable: ");
    return resolver == null ? delegate.read(path, limit) : resolver.read(path, limit);
  }

  @Override
  public List<String> readLines(FsPath path, int limit) throws SQLException {
    VirtualDirectoryResolver resolver = readableResolver(path, "Path is not readable as text: ");
    return resolver == null ? delegate.readLines(path, limit) : resolver.readLines(path, limit);
  }

  @Override
  public List<SqlRow> tail(FsPath path, int limit) throws SQLException {
    VirtualDirectoryResolver resolver = readableResolver(path, "Path does not support tail: ");
    return resolver == null ? delegate.tail(path, limit) : resolver.tail(path, limit);
  }

  @Override
  public List<String> tailLines(FsPath path, int limit) throws SQLException {
    VirtualDirectoryResolver resolver = readableResolver(path, "Path does not support tail: ");
    return resolver == null ? delegate.tailLines(path, limit) : resolver.tailLines(path, limit);
  }

  @Override
  public long count(FsPath path) throws SQLException {
    VirtualDirectoryResolver resolver = readableResolver(path, "Path does not support count: ");
    return resolver == null ? delegate.count(path) : resolver.count(path);
  }

  @Override
  public List<SqlRow> read(List<FsPath> paths, int limit) throws SQLException {
    if (paths.size() == 1) {
      return read(paths.get(0), limit);
    }
    for (FsPath path : paths) {
      if (VirtualDirectoryPaths.isVirtualPath(path)) {
        throw new SQLException("Multiple paths are not readable when virtual paths are included");
      }
    }
    return delegate.read(paths, limit);
  }

  private List<FsNode> listRoot(FsPath path) throws SQLException {
    List<FsNode> nodes = new ArrayList<>();
    for (FsNode node : delegate.list(path)) {
      if (!VirtualDirectoryPaths.VIRTUAL_ROOT_NAME.equals(node.getName())) {
        nodes.add(node);
      }
    }
    nodes.add(VirtualDirectoryPaths.virtualRootNode());
    return nodes;
  }

  private List<FsNode> listVirtualRoots() {
    List<FsNode> nodes = new ArrayList<>();
    for (VirtualDirectoryResolver resolver : resolvers.values()) {
      nodes.add(resolver.rootNode());
    }
    return nodes;
  }

  private VirtualDirectoryResolver resolver(FsPath path) {
    if (!VirtualDirectoryPaths.isVirtualPath(path) || VirtualDirectoryPaths.isVirtualRoot(path)) {
      return null;
    }
    return resolvers.get(VirtualDirectoryPaths.resolverName(path));
  }

  private VirtualDirectoryResolver readableResolver(FsPath path, String errorPrefix)
      throws SQLException {
    if (!VirtualDirectoryPaths.isVirtualPath(path)) {
      return null;
    }
    VirtualDirectoryResolver resolver = resolver(path);
    if (resolver == null) {
      throw new SQLException(errorPrefix + path);
    }
    return resolver;
  }
}
