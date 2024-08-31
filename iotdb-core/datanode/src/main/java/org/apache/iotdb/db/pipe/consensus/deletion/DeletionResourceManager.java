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

package org.apache.iotdb.db.pipe.consensus.deletion;

import org.apache.iotdb.commons.consensus.index.ProgressIndex;
import org.apache.iotdb.commons.consensus.index.impl.SimpleProgressIndex;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.pipe.PipeConsensus;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.pipe.consensus.ProgressIndexDataNodeManager;
import org.apache.iotdb.db.pipe.consensus.deletion.persist.DeletionBuffer;
import org.apache.iotdb.db.pipe.consensus.deletion.recover.DeletionReader;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class DeletionResourceManager implements AutoCloseable {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeletionResourceManager.class);
  public static final String DELETION_FILE_SUFFIX = ".deletion";
  public static final String MAGIC_VERSION_V1 = "DELETION_V1";
  private static final String REBOOT_TIME = "REBOOT_TIME";
  private static final String MEM_TABLE_FLUSH_ORDER = "MEM_TABLE_FLUSH_ORDER";
  private static final String DELETION_FILE_NAME_PATTERN =
      String.format(
          "^_(?<%s>\\d+)_(?<%s>\\d+)\\%s$",
          REBOOT_TIME, MEM_TABLE_FLUSH_ORDER, DELETION_FILE_SUFFIX);
  private final String dataRegionId;
  private final DeletionBuffer deletionBuffer;
  private final File storageDir;
  private final List<DeletionResource> deletionResources = new CopyOnWriteArrayList<>();

  public DeletionResourceManager(String dataRegionId) throws IOException {
    this.dataRegionId = dataRegionId;
    this.storageDir =
        new File(
            IoTDBDescriptor.getInstance().getConfig().getPipeConsensusDeletionFileDir()
                + File.separator
                + dataRegionId);
    this.deletionBuffer = new DeletionBuffer(dataRegionId, storageDir.getAbsolutePath());
    initAndRecover();
    // Only after initAndRecover can we start serialize and sync new deletions.
    this.deletionBuffer.start();
  }

  private void initAndRecover() throws IOException {
    if (!storageDir.exists()) {
      // Init
      if (!storageDir.mkdirs()) {
        LOGGER.warn("Unable to create pipeConsensus deletion dir at {}", storageDir);
        throw new IOException(
            String.format("Unable to create pipeConsensus deletion dir at %s", storageDir));
      }
    }
    try (Stream<Path> pathStream = Files.walk(Paths.get(storageDir.getPath()), 1)) {
      Path[] deletionPaths =
          pathStream
              .filter(Files::isRegularFile)
              .filter(path -> path.getFileName().toString().matches(DELETION_FILE_NAME_PATTERN))
              .toArray(Path[]::new);

      for (Path path : deletionPaths) {
        try (DeletionReader deletionReader =
            new DeletionReader(path.toFile(), this::removeDeletionResource)) {
          deletionResources.addAll(deletionReader.readAllDeletions());
        }
      }
    }
  }

  @Override
  public void close() throws Exception {
    this.deletionBuffer.close();
    this.deletionResources.clear();
  }

  public void registerDeletionResource(PipeSchemaRegionWritePlanEvent event) {
    DeletionResource deletionResource = new DeletionResource(event, this::removeDeletionResource);
    event.setDeletionResource(deletionResource);
    this.deletionResources.add(deletionResource);
    deletionBuffer.registerDeletionResource(deletionResource);
  }

  public List<DeletionResource> getAllDeletionResources() {
    return deletionResources.stream().collect(ImmutableList.toImmutableList());
  }

  /**
   * This is a hook function, which will be automatically invoked when deletionResource's reference
   * count returns to 0.
   */
  private void removeDeletionResource(DeletionResource deletionResource) {
    // Clean memory
    deletionResources.remove(deletionResource);
    // Clean disk
    ProgressIndex currentProgressIndex =
        ProgressIndexDataNodeManager.extractLocalSimpleProgressIndex(
            deletionResource.getProgressIndex());

    try (Stream<Path> pathStream = Files.walk(Paths.get(storageDir.getPath()), 1)) {
      Path[] deletionPaths =
          pathStream
              .filter(Files::isRegularFile)
              .filter(path -> path.getFileName().toString().matches(DELETION_FILE_NAME_PATTERN))
              .filter(
                  path ->
                      isCurrentFileCanBeDeleted(
                          path.getFileName().toString(), currentProgressIndex))
              .toArray(Path[]::new);
      for (Path path : deletionPaths) {
        FileUtils.deleteFileOrDirectory(path.toFile());
      }
    } catch (IOException e) {
      LOGGER.warn(
          "DeletionManager-{} failed to delete file in {} dir, please manually check!",
          dataRegionId,
          storageDir);
    }
  }

  private boolean isCurrentFileCanBeDeleted(String fileName, ProgressIndex currentProgressIndex) {
    if (currentProgressIndex instanceof SimpleProgressIndex) {
      SimpleProgressIndex simpleProgressIndex = (SimpleProgressIndex) currentProgressIndex;
      int curRebootTimes = simpleProgressIndex.getRebootTimes();
      long curMemTableFlushOrderId = simpleProgressIndex.getMemTableFlushOrderId();

      Pattern pattern = Pattern.compile(DELETION_FILE_NAME_PATTERN);
      Matcher matcher = pattern.matcher(fileName);
      // Definitely match. Because upper caller has filtered fileNames.
      if (matcher.matches()) {
        int fileRebootTimes = Integer.parseInt(matcher.group(REBOOT_TIME));
        long fileMemTableFlushOrderId = Long.parseLong(matcher.group(MEM_TABLE_FLUSH_ORDER));
        return fileRebootTimes == curRebootTimes
            ? fileMemTableFlushOrderId < curMemTableFlushOrderId
            : fileRebootTimes < curMemTableFlushOrderId;
      }
    }
    return false;
  }

  //////////////////////////// singleton ////////////////////////////
  private static class DeletionResourceManagerHolder {
    private static Map<String, DeletionResourceManager> CONSENSU_GROUP_ID_2_INSTANCE_MAP;

    private DeletionResourceManagerHolder() {}

    public static void build() {
      if (CONSENSU_GROUP_ID_2_INSTANCE_MAP == null) {
        CONSENSU_GROUP_ID_2_INSTANCE_MAP = new ConcurrentHashMap<>();
      }
    }
  }

  public static DeletionResourceManager getInstance(String groupId) {
    // If consensusImpl is not PipeConsensus.
    if (DeletionResourceManagerHolder.CONSENSU_GROUP_ID_2_INSTANCE_MAP == null) {
      return null;
    }
    return DeletionResourceManagerHolder.CONSENSU_GROUP_ID_2_INSTANCE_MAP.computeIfAbsent(
        groupId,
        key -> {
          try {
            return new DeletionResourceManager(groupId);
          } catch (IOException e) {
            LOGGER.error("Failed to initialize DeletionResourceManager", e);
            throw new RuntimeException(e);
          }
        });
  }

  // Only when consensus protocol is PipeConsensus, will this class be initialized.
  public static void build() {
    if (DataRegionConsensusImpl.getInstance() instanceof PipeConsensus) {
      DeletionResourceManagerHolder.build();
    }
  }
}
