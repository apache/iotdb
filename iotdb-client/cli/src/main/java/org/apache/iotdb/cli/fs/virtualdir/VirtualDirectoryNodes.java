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
import org.apache.iotdb.cli.fs.node.FsNodeType;
import org.apache.iotdb.cli.fs.path.FsPath;

import java.util.LinkedHashMap;
import java.util.Map;

final class VirtualDirectoryNodes {

  private VirtualDirectoryNodes() {}

  static FsNode root(String name, String description) {
    Map<String, String> metadata = new LinkedHashMap<>();
    metadata.put("kind", "virtual-directory");
    metadata.put("description", description);
    return new FsNode(
        name, VirtualDirectoryPaths.resolverRootPath(name), FsNodeType.VIRTUAL_DIRECTORY, metadata);
  }

  static FsNode directory(
      String name, FsPath path, String resolverName, String canonicalPath, String kind) {
    Map<String, String> metadata = new LinkedHashMap<>();
    metadata.put("kind", kind);
    metadata.put("virtualDirectory", resolverName);
    if (canonicalPath != null && !canonicalPath.isEmpty()) {
      metadata.put("canonicalPath", canonicalPath);
    }
    return new FsNode(name, path, FsNodeType.VIRTUAL_DIRECTORY, metadata);
  }

  static FsNode directory(
      String name,
      FsPath path,
      String resolverName,
      String canonicalPath,
      String kind,
      Map<String, String> extraMetadata) {
    Map<String, String> metadata = new LinkedHashMap<>(extraMetadata);
    metadata.put("kind", kind);
    metadata.put("virtualDirectory", resolverName);
    if (canonicalPath != null && !canonicalPath.isEmpty()) {
      metadata.put("canonicalPath", canonicalPath);
    }
    return new FsNode(name, path, FsNodeType.VIRTUAL_DIRECTORY, metadata);
  }

  static FsNode rewrite(FsNode node, FsPath virtualPath, String resolverName) {
    return rewrite(node, node.getName(), virtualPath, resolverName, new LinkedHashMap<>());
  }

  static FsNode rewrite(
      FsNode node,
      String virtualName,
      FsPath virtualPath,
      String resolverName,
      Map<String, String> extraMetadata) {
    if (node.getType() == FsNodeType.UNKNOWN) {
      return VirtualDirectoryPaths.unknown(virtualPath);
    }
    Map<String, String> metadata = new LinkedHashMap<>(node.getMetadata());
    metadata.putAll(extraMetadata);
    metadata.put("virtualDirectory", resolverName);
    metadata.put("canonicalPath", node.getPath().toString());
    return new FsNode(virtualName, virtualPath, node.getType(), metadata);
  }
}
