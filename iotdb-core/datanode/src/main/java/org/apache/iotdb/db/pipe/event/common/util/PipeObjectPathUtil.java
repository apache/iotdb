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

package org.apache.iotdb.db.pipe.event.common.util;

import org.apache.iotdb.calc.utils.ObjectTypeUtils;
import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.common.constant.TsFileConstant;
import org.apache.tsfile.utils.Pair;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Stream;

public final class PipeObjectPathUtil {

  private PipeObjectPathUtil() {}

  public static File resolveLinkedObjectFile(
      final TsFileResource tsFileResource, final String pipeName, final String relativePath) {
    Objects.requireNonNull(pipeName, "pipeName must not be null");

    if (tsFileResource == null || relativePath == null || relativePath.trim().isEmpty()) {
      return null;
    }

    return PipeDataNodeResourceManager.object()
        .getObjectFileHardlink(tsFileResource, relativePath, pipeName);
  }

  public static File resolveLinkedObjectDirectory(
      final TsFileResource tsFileResource, final String pipeName) {
    Objects.requireNonNull(pipeName, "pipeName must not be null");

    if (tsFileResource == null || tsFileResource.getTsFile() == null) {
      return null;
    }

    return PipeDataNodeResourceManager.object().getLinkedObjectDirectory(tsFileResource, pipeName);
  }

  public static String tsFileBaseNameWithoutSuffix(final String tsFileName) {
    if (tsFileName == null || tsFileName.isEmpty()) {
      return tsFileName;
    }
    final Path fileNamePath = Paths.get(tsFileName).getFileName();
    final String leaf = fileNamePath != null ? fileNamePath.toString() : tsFileName;
    final String suffix = TsFileConstant.TSFILE_SUFFIX;
    if (leaf.endsWith(suffix)) {
      final int suffixIndex = leaf.lastIndexOf(suffix);
      if (suffixIndex > 0) {
        return leaf.substring(0, suffixIndex);
      }
    }
    return leaf;
  }

  public static Path toRelativePath(final String[] pathSegments) {
    Path relativePath = Paths.get("");
    if (pathSegments == null || pathSegments.length == 0) {
      return relativePath;
    }
    for (final String segment : pathSegments) {
      relativePath = relativePath.resolve(segment);
    }
    return relativePath.normalize();
  }

  public static String[] toPathSegments(final Path relativePath) {
    final List<String> segments = new ArrayList<>();
    for (final Path name : relativePath) {
      segments.add(name.toString());
    }
    return segments.toArray(new String[0]);
  }

  public static Stream<Pair<Path, File>> getObjectFileStream(final Path rootDir)
      throws IOException {
    if (rootDir == null || !Files.isDirectory(rootDir)) {
      return Stream.empty();
    }
    return Files.find(
            rootDir,
            Integer.MAX_VALUE,
            (path, attrs) ->
                attrs.isRegularFile()
                    && path.getFileName().toString().endsWith(ObjectTypeUtils.OBJECT_FILE_SUFFIX))
        .map(path -> new Pair<>(rootDir.relativize(path).normalize(), path.toFile()));
  }

  public static String combineTsFileBaseWithPortableRelative(
      final String tsFileNameWithoutSuffix, final String[] relativeSegments) {
    final Path relativeUnderObjectDir = toRelativePath(relativeSegments);
    if (relativeUnderObjectDir.getNameCount() == 0) {
      return tsFileNameWithoutSuffix != null ? tsFileNameWithoutSuffix : "";
    }
    if (tsFileNameWithoutSuffix == null || tsFileNameWithoutSuffix.isEmpty()) {
      return relativeUnderObjectDir.toString();
    }
    final Path combined =
        Paths.get(tsFileNameWithoutSuffix).resolve(relativeUnderObjectDir).normalize();
    return combined.toString();
  }
}
