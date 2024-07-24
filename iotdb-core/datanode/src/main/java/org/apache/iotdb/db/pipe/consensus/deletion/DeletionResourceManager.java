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

import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.consensus.pipe.PipeConsensus;
import org.apache.iotdb.db.consensus.DataRegionConsensusImpl;
import org.apache.iotdb.db.pipe.agent.PipeDataNodeAgent;
import org.apache.iotdb.db.pipe.event.common.schema.PipeSchemaRegionWritePlanEvent;

import com.google.common.collect.ImmutableList;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.DataOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;

public class DeletionResourceManager {
  private static final Logger LOGGER = LoggerFactory.getLogger(DeletionResourceManager.class);
  private static final long DELETION_TIME_TO_LIVE_IN_MS = 1000L;
  private static final long CHECK_DELETION_DURATION_IN_MS = 1000L * 5;
  private static final String DELETION_CHECKER_TASK_ID = "pipe_consensus_deletion_checker";
  private static final String DELETION_FILE_SUFFIX = ".deletion";
  // TODO: read it from conf
  private final File storageDir = new File("tmp");
  private final List<DeletionResource> deletionResources = new CopyOnWriteArrayList<>();

  public DeletionResourceManager() throws IOException {
    initAndRecover();
    // Register scheduled deletion check task.
    PipeDataNodeAgent.runtime()
        .registerPeriodicalJob(
            DELETION_CHECKER_TASK_ID, this::checkAndCleanDeletions, CHECK_DELETION_DURATION_IN_MS);
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
              .filter(path -> path.endsWith(DELETION_FILE_SUFFIX))
              .toArray(Path[]::new);
      ByteBuffer readBuffer;
      for (Path path : deletionPaths) {
        readBuffer = ByteBuffer.wrap(Files.readAllBytes(path));
        deletionResources.add(
            DeletionResource.deserialize(readBuffer, this::persist, this::removeDeletionResource));
      }
    }
  }

  public void registerDeletionResource(PipeSchemaRegionWritePlanEvent event) {
    DeletionResource deletionResource =
        new DeletionResource(event, this::persist, this::removeDeletionResource);
    event.setDeletionResource(deletionResource);
    this.deletionResources.add(deletionResource);
  }

  public List<DeletionResource> getAllDeletionResources() {
    return deletionResources.stream().collect(ImmutableList.toImmutableList());
  }

  public void persist(final DeletionResource deletionResource) {
    File deletionFile =
        new File(
            storageDir, String.format("%d%s", deletionResource.hashCode(), DELETION_FILE_SUFFIX));
    if (deletionFile.exists()) {
      LOGGER.warn("Deletion file {} already exists, delete it.", deletionFile);
      FileUtils.deleteFileOrDirectory(deletionFile);
    }

    try (FileOutputStream fileOutputStream = new FileOutputStream(deletionFile)) {
      try (DataOutputStream dataOutputStream = new DataOutputStream(fileOutputStream)) {
        final ByteBuffer byteBuffer = deletionResource.serialize();
        ReadWriteIOUtils.write(byteBuffer, dataOutputStream);
      } finally {
        try {
          fileOutputStream.flush();
          fileOutputStream.getFD().sync();
        } catch (IOException ignore) {
          // ignore sync exception
        }
      }
    } catch (IOException e) {
      // log error and ignore exception
      LOGGER.error(
          "Failed to persist deletion resource {}, may cause inconsistency during replication",
          deletionResource,
          e);
    }
  }

  private void removeDeletionResource(DeletionResource deletionResource) {
    // TODO: 参考 WAL 的水位线机制，攒批之类的，参数也可以参考 WAL
    // TODO: 删除文件也可以通过考虑利用 progressIndex 来删
    // TODO: 需要考虑删除后的 index 维护，恢复之类的
  }

  private void checkAndCleanDeletions() {
    final ImmutableList<DeletionResource> toBeCleaned =
        deletionResources.stream()
            .filter(deletionResource -> deletionResource.getReferenceCount() == 0)
            .collect(ImmutableList.toImmutableList());

    toBeCleaned.forEach(
        deletionResource -> {
          // Clean disk
          File deletionFile =
              new File(
                  storageDir,
                  String.format("%d%s", deletionResource.hashCode(), DELETION_FILE_SUFFIX));
          if (deletionFile.exists()) {
            FileUtils.deleteFileOrDirectory(deletionFile);
          }
          // Clean memory
          deletionResources.remove(deletionResource);
          deletionResource.releaseSelf();
        });
  }

  //////////////////////////// singleton ////////////////////////////
  private static class DeletionResourceManagerHolder {
    private static DeletionResourceManager INSTANCE;

    private DeletionResourceManagerHolder() {}

    public static void build() throws IOException {
      if (INSTANCE == null) {
        INSTANCE = new DeletionResourceManager();
      }
    }
  }

  public static DeletionResourceManager getInstance() {
    return DeletionResourceManager.DeletionResourceManagerHolder.INSTANCE;
  }

  // Only when consensus protocol is PipeConsensus, will this class be initialized.
  public static void build() {
    if (DataRegionConsensusImpl.getInstance() instanceof PipeConsensus) {
      try {
        DeletionResourceManagerHolder.build();
      } catch (IOException e) {
        LOGGER.error("Failed to initialize DeletionResourceManager", e);
        throw new RuntimeException(e);
      }
    }
  }
}
