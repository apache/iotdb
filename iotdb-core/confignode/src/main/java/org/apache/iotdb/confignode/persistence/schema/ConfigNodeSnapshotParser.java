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

package org.apache.iotdb.confignode.persistence.schema;

import org.apache.iotdb.commons.conf.IoTDBConstant;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.confignode.conf.ConfigNodeConfig;
import org.apache.iotdb.confignode.conf.ConfigNodeDescriptor;
import org.apache.iotdb.confignode.i18n.ConfigNodeMessages;
import org.apache.iotdb.confignode.persistence.TTLInfo;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class ConfigNodeSnapshotParser {
  private static final Logger LOGGER = LoggerFactory.getLogger(ConfigNodeSnapshotParser.class);
  private static final ConfigNodeConfig CONF = ConfigNodeDescriptor.getInstance().getConf();

  private static final String SNAPSHOT_CLUSTER_SCHEMA_FILENAME = "cluster_schema.bin";
  private static final String SNAPSHOT_TABLE_CLUSTER_SCHEMA_FILENAME = "table_cluster_schema.bin";
  private static final String SNAPSHOT_TEMPLATE_FILENAME = "template_info.bin";

  private ConfigNodeSnapshotParser() {
    // Empty constructor
  }

  public static class SnapshotPathInfo {

    private final Path snapshotPath;
    private final Path templatePath;
    private final Path pipeRenameTimeSeriesPath;
    private final CNSnapshotFileType type;

    public SnapshotPathInfo(
        final Path snapshotPath,
        final Path templatePath,
        final Path pipeRenameTimeSeriesPath,
        final CNSnapshotFileType type) {
      this.snapshotPath = snapshotPath;
      this.templatePath = templatePath;
      this.pipeRenameTimeSeriesPath = pipeRenameTimeSeriesPath;
      this.type = type;
    }

    public Path getSnapshotPath() {
      return snapshotPath;
    }

    public Path getTemplatePath() {
      return templatePath;
    }

    public Path getPipeRenameTimeSeriesPath() {
      return pipeRenameTimeSeriesPath;
    }

    public CNSnapshotFileType getType() {
      return type;
    }
  }

  private static Path getLatestSnapshotPath(final List<Path> snapshotPathList) {
    if (snapshotPathList.isEmpty()) {
      return null;
    }
    final Path[] pathArray = snapshotPathList.toArray(new Path[0]);
    Arrays.sort(
        pathArray,
        (o1, o2) -> {
          final String index1 = o1.toFile().getName().split("_")[1];
          final String index2 = o2.toFile().getName().split("_")[1];
          return Long.compare(Long.parseLong(index2), Long.parseLong(index1));
        });
    return pathArray[0];
  }

  public static List<SnapshotPathInfo> getSnapshots() throws IOException {
    final List<SnapshotPathInfo> snapshotPairList = new ArrayList<>();
    final String snapshotPath = CONF.getConsensusDir();
    try (final DirectoryStream<Path> stream =
        Files.newDirectoryStream(Paths.get(snapshotPath), "[0-9]*-[0-9]*-[0-9]*-[0-9]*-[0-9]*")) {
      // In confignode there is only one consensus dir.
      // Get into confignode consensus dir
      for (final Path path : stream) {
        try (final DirectoryStream<Path> filestream =
            Files.newDirectoryStream(Paths.get(path.toString() + File.separator + "sm"))) {
          final ArrayList<Path> snapshotList = new ArrayList<>();
          for (final Path snapshotFolder : filestream) {
            if (snapshotFolder.toFile().isDirectory()) {
              snapshotList.add(snapshotFolder);
            }
          }
          final Path latestSnapshotPath = getLatestSnapshotPath(snapshotList);

          if (latestSnapshotPath != null) {
            // Get role files.
            final String rolePath =
                latestSnapshotPath
                    + File.separator
                    + IoTDBConstant.SYSTEM_FOLDER_NAME
                    + File.separator
                    + "roles";
            try (final DirectoryStream<Path> roleStream =
                Files.newDirectoryStream(Paths.get(rolePath))) {
              for (final Path role : roleStream) {
                snapshotPairList.add(
                    new SnapshotPathInfo(role, null, null, CNSnapshotFileType.ROLE));
              }
            }
            // Get user files.
            final String userPath =
                latestSnapshotPath
                    + File.separator
                    + IoTDBConstant.SYSTEM_FOLDER_NAME
                    + File.separator
                    + "users";
            try (final DirectoryStream<Path> userStream =
                Files.newDirectoryStream(Paths.get(userPath))) {
              final List<Path> userFilePath = new ArrayList<>();
              final List<Path> userRoleFilePath = new ArrayList<>();
              for (final Path user : userStream) {
                if (user.getFileName().toString().contains("_role.profile")) {
                  userRoleFilePath.add(user);
                } else {
                  userFilePath.add(user);
                }
              }
              // We should add user file firstly.
              for (final Path user : userFilePath) {
                snapshotPairList.add(
                    new SnapshotPathInfo(user, null, null, CNSnapshotFileType.USER));
              }
              for (final Path roleList : userRoleFilePath) {
                snapshotPairList.add(
                    new SnapshotPathInfo(roleList, null, null, CNSnapshotFileType.USER_ROLE));
              }
            }

            // Get cluster schema info file and template file.
            final File schemaInfoFile =
                SystemFileFactory.INSTANCE.getFile(
                    latestSnapshotPath + File.separator + SNAPSHOT_CLUSTER_SCHEMA_FILENAME);
            final File templateInfoFile =
                SystemFileFactory.INSTANCE.getFile(
                    latestSnapshotPath + File.separator + SNAPSHOT_TEMPLATE_FILENAME);
            final File pipeRenameTimeSeriesFile =
                SystemFileFactory.INSTANCE.getFile(
                    latestSnapshotPath
                        + File.separator
                        + ClusterSchemaInfo.PIPE_RENAME_TIME_SERIES_SNAPSHOT_FILENAME);
            if (schemaInfoFile.exists() && templateInfoFile.exists()) {
              snapshotPairList.add(
                  new SnapshotPathInfo(
                      schemaInfoFile.toPath(),
                      templateInfoFile.toPath(),
                      pipeRenameTimeSeriesFile.exists() ? pipeRenameTimeSeriesFile.toPath() : null,
                      CNSnapshotFileType.SCHEMA));
            }

            // Get table schema info file
            final File tableInfoFile =
                SystemFileFactory.INSTANCE.getFile(
                    latestSnapshotPath + File.separator + SNAPSHOT_TABLE_CLUSTER_SCHEMA_FILENAME);
            if (tableInfoFile.exists()) {
              snapshotPairList.add(
                  new SnapshotPathInfo(
                      tableInfoFile.toPath(), null, null, CNSnapshotFileType.SCHEMA));
            }

            // Get ttl info file
            final File ttlInfoFile =
                SystemFileFactory.INSTANCE.getFile(
                    latestSnapshotPath + File.separator + TTLInfo.SNAPSHOT_FILENAME);
            if (ttlInfoFile.exists()) {
              snapshotPairList.add(
                  new SnapshotPathInfo(ttlInfoFile.toPath(), null, null, CNSnapshotFileType.TTL));
            }
          }
        }
      }
    }
    return snapshotPairList;
  }

  public static CNPhysicalPlanGenerator translate2PhysicalPlan(
      final Path path1, final Path path2, final CNSnapshotFileType type, final String userName)
      throws IOException {
    return translate2PhysicalPlan(path1, path2, null, type, userName);
  }

  public static CNPhysicalPlanGenerator translate2PhysicalPlan(
      final Path path1,
      final Path path2,
      final Path pipeRenameTimeSeriesPath,
      final CNSnapshotFileType type,
      final String userName)
      throws IOException {
    if (path1 == null) {
      LOGGER.warn(ConfigNodeMessages.PATH1_SHOULD_NOT_BE_NULL);
      return null;
    }

    if (!path1.toFile().exists()) {
      LOGGER.warn(ConfigNodeMessages.FILE_NOT_EXISTS, path1.toFile().getName());
      return null;
    }

    if (type == CNSnapshotFileType.SCHEMA) {
      return new CNPhysicalPlanGenerator(path1, path2, pipeRenameTimeSeriesPath);
    } else {
      return new CNPhysicalPlanGenerator(path1, type, userName);
    }
  }
}
