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

package org.apache.iotdb.db.tools.schema;

import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.tsfile.utils.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class SchemaRegionSnapshotParser {

  private static final Logger LOGGER = LoggerFactory.getLogger(SchemaRegionSnapshotParser.class);

  private static final IoTDBConfig CONFIG = IoTDBDescriptor.getInstance().getConfig();

  private SchemaRegionSnapshotParser() {
    // Empty constructor
  }

  private static Path getLatestSnapshotPath(
      final List<Path> snapshotPathList, final boolean includingTmp) {
    if (snapshotPathList.isEmpty()) {
      return null;
    }
    final Path[] pathArray = snapshotPathList.toArray(new Path[0]);
    Arrays.sort(
        pathArray,
        (o1, o2) -> {
          // There will be only one temp file during snapshot
          if (includingTmp && o1.toString().contains(".tmp.")) {
            return 1;
          }
          final String index1 = o1.toFile().getName().split("_")[1];
          final String index2 = o2.toFile().getName().split("_")[1];
          return Long.compare(Long.parseLong(index1), Long.parseLong(index2));
        });
    return pathArray[pathArray.length - 1];
  }

  // In schema snapshot path: datanode/consensus/schema_region/47474747-4747-4747-4747-000200000000
  // this func will get schema region id = 47474747-4747-4747-4747-000200000000's latest snapshot.
  // In one schema region, there is only one snapshot unit.
  public static Pair<Path, Path> getSnapshotPaths(
      final String schemaRegionId, final boolean isTmp) {
    final String snapshotPath = CONFIG.getSchemaRegionConsensusDir();
    final File snapshotDir =
        new File(snapshotPath + File.separator + schemaRegionId + File.separator + "sm");

    // Get the latest snapshot file
    final ArrayList<Path> snapshotList = new ArrayList<>();
    try (final DirectoryStream<Path> stream =
        Files.newDirectoryStream(
            snapshotDir.toPath(), isTmp ? ".tmp.[0-9]*_[0-9]*" : "[0-9]*_[0-9]*")) {
      for (final Path path : stream) {
        snapshotList.add(path);
      }
    } catch (final IOException ioException) {
      LOGGER.warn("ioexception when get {}'s folder", schemaRegionId, ioException);
      return null;
    }
    final Path latestSnapshotPath = getLatestSnapshotPath(snapshotList, isTmp);
    if (latestSnapshotPath != null) {
      // Get metadata from the latest snapshot folder.
      final File mTreeSnapshot =
          SystemFileFactory.INSTANCE.getFile(
              latestSnapshotPath + File.separator + SchemaConstant.MTREE_SNAPSHOT);
      final File tagSnapshot =
          SystemFileFactory.INSTANCE.getFile(
              latestSnapshotPath + File.separator + SchemaConstant.TAG_LOG_SNAPSHOT);
      if (mTreeSnapshot.exists()) {
        return new Pair<>(
            mTreeSnapshot.toPath(), tagSnapshot.exists() ? tagSnapshot.toPath() : null);
      }
    }
    return null;
  }

  public static SRStatementGenerator translate2Statements(
      final Path mtreePath, final Path tagFilePath, final PartialPath databasePath)
      throws IOException {
    if (mtreePath == null) {
      return null;
    }
    final File mtreefile = mtreePath.toFile();
    final File tagfile =
        tagFilePath != null && tagFilePath.toFile().exists() ? tagFilePath.toFile() : null;

    if (!mtreefile.exists()) {
      return null;
    }

    if (!mtreefile.getName().equals(SchemaConstant.MTREE_SNAPSHOT)) {
      throw new IllegalArgumentException(
          String.format(
              "%s is not allowed, only support %s",
              mtreefile.getName(), SchemaConstant.MTREE_SNAPSHOT));
    }
    if (tagfile != null && !tagfile.getName().equals(SchemaConstant.TAG_LOG_SNAPSHOT)) {
      throw new IllegalArgumentException(
          String.format(
              " %s is not allowed, only support %s",
              tagfile.getName(), SchemaConstant.TAG_LOG_SNAPSHOT));
    }
    return new SRStatementGenerator(mtreefile, tagfile, databasePath);
  }
}
