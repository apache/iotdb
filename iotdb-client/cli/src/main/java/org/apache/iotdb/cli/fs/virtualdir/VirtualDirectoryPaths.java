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

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

final class VirtualDirectoryPaths {

  static final String VIRTUAL_ROOT_NAME = ".virtual";
  static final FsPath VIRTUAL_ROOT_PATH = FsPath.absolute("/" + VIRTUAL_ROOT_NAME);

  private VirtualDirectoryPaths() {}

  static boolean isVirtualRoot(FsPath path) {
    List<String> segments = path.getSegments();
    return segments.size() == 1 && VIRTUAL_ROOT_NAME.equals(segments.get(0));
  }

  static boolean isVirtualPath(FsPath path) {
    List<String> segments = path.getSegments();
    return !segments.isEmpty() && VIRTUAL_ROOT_NAME.equals(segments.get(0));
  }

  static String resolverName(FsPath path) {
    List<String> segments = path.getSegments();
    return segments.size() < 2 ? "" : segments.get(1);
  }

  static List<String> resolverSegments(FsPath path) {
    List<String> segments = path.getSegments();
    if (segments.size() <= 2) {
      return new ArrayList<>();
    }
    return new ArrayList<>(segments.subList(2, segments.size()));
  }

  static FsPath resolverRootPath(String resolverName) {
    return FsPath.absolute("/" + VIRTUAL_ROOT_NAME + "/" + resolverName);
  }

  static FsPath virtualPath(String resolverName, List<String> segments) {
    StringBuilder builder = new StringBuilder();
    builder.append('/').append(VIRTUAL_ROOT_NAME).append('/').append(resolverName);
    for (String segment : segments) {
      builder.append('/').append(segment);
    }
    return FsPath.absolute(builder.toString());
  }

  static FsNode virtualRootNode() {
    Map<String, String> metadata = new LinkedHashMap<>();
    metadata.put("kind", "virtual-root");
    return new FsNode(VIRTUAL_ROOT_NAME, VIRTUAL_ROOT_PATH, FsNodeType.VIRTUAL_DIRECTORY, metadata);
  }

  static FsNode directory(String name, FsPath path, String kind) {
    Map<String, String> metadata = new LinkedHashMap<>();
    metadata.put("kind", kind);
    return new FsNode(name, path, FsNodeType.VIRTUAL_DIRECTORY, metadata);
  }

  static FsNode unknown(FsPath path) {
    return new FsNode(path.getFileName(), path, FsNodeType.UNKNOWN);
  }
}
