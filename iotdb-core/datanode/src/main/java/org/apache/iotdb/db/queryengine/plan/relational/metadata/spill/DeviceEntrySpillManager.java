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

package org.apache.iotdb.db.queryengine.plan.relational.metadata.spill;

import org.apache.iotdb.commons.queryengine.plan.planner.plan.node.PlanNodeId;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import java.io.IOException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

public final class DeviceEntrySpillManager {

  private final ConcurrentHashMap<String, Set<Path>> queryDirectories = new ConcurrentHashMap<>();

  private DeviceEntrySpillManager() {}

  public static DeviceEntrySpillManager getInstance() {
    return DeviceEntrySpillManagerHolder.INSTANCE;
  }

  public Path register(String queryId, PlanNodeId planNodeId) throws IOException {
    Path ownerDirectory = resolveOwnerDirectory(queryId, planNodeId.getId());
    Files.createDirectories(ownerDirectory);
    queryDirectories
        .computeIfAbsent(queryId, ignored -> ConcurrentHashMap.newKeySet())
        .add(ownerDirectory);
    return ownerDirectory;
  }

  public void deregisterOwner(String queryId, Path ownerDirectory) throws IOException {
    deleteDirectoryIfExists(ownerDirectory);
    Set<Path> owners = queryDirectories.get(queryId);
    if (owners != null) {
      owners.remove(ownerDirectory);
      if (owners.isEmpty()) {
        queryDirectories.remove(queryId, owners);
        deleteDirectoryIfExists(resolveQueryDirectory(queryId));
      }
    }
  }

  public void deregisterQuery(String queryId) throws IOException {
    queryDirectories.remove(queryId);
    deleteDirectoryIfExists(resolveQueryDirectory(queryId));
  }

  public byte[] readSegment(String queryId, String planNodeId, int segmentId) throws IOException {
    return Files.readAllBytes(resolveSegment(queryId, planNodeId, segmentId));
  }

  public Path resolveSegment(String queryId, String planNodeId, int segmentId) throws IOException {
    Path segment = getRegisteredSegmentPath(queryId, planNodeId, segmentId);
    if (!Files.isRegularFile(segment)) {
      throw new java.nio.file.NoSuchFileException(segment.toString());
    }
    return segment;
  }

  public void deleteSegment(String queryId, String dataSetId, int segmentId) throws IOException {
    try {
      Files.deleteIfExists(getRegisteredSegmentPath(queryId, dataSetId, segmentId));
    } catch (NoSuchFileException e) {
      // Query cleanup may have already removed the whole query directory.
    }
  }

  public void deleteSegment(String queryId, PlanNodeId planNodeId, int segmentId)
      throws IOException {
    deleteSegment(queryId, planNodeId.getId(), segmentId);
  }

  public void finishSegmentDataSet(String queryId, String planNodeId) throws IOException {
    Path ownerDirectory = resolveOwnerDirectory(queryId, planNodeId);
    deregisterOwner(queryId, ownerDirectory);
  }

  private Path resolveRegisteredDataSetDirectory(String queryId, String planNodeId)
      throws IOException {
    Path relativeDataSetPath = Path.of(planNodeId);
    Path queryDirectory = resolveQueryDirectory(queryId);
    Path dataSetDirectory = queryDirectory.resolve(relativeDataSetPath).resolve("fi").normalize();
    if (!dataSetDirectory.startsWith(queryDirectory)) {
      throw new IllegalArgumentException();
    }
    Set<Path> owners = queryDirectories.get(queryId);
    boolean registered =
        owners != null
            && owners.stream()
                .map(Path::normalize)
                .anyMatch(owner -> dataSetDirectory.startsWith(owner) && Files.isDirectory(owner));
    if (!registered || !Files.isDirectory(dataSetDirectory)) {
      throw new java.nio.file.NoSuchFileException(dataSetDirectory.toString());
    }
    return dataSetDirectory;
  }

  private Path getRegisteredSegmentPath(String queryId, String planNodeId, int segmentId)
      throws IOException {
    if (segmentId < 0) {
      throw new IllegalArgumentException();
    }
    return resolveRegisteredDataSetDirectory(queryId, planNodeId)
        .resolve(String.format("segment-%06d.bin", segmentId));
  }

  public synchronized void clearStaleData() throws IOException {
    deleteDirectoryIfExists(rootDirectory());
    Files.createDirectories(rootDirectory());
    queryDirectories.clear();
  }

  private void deleteDirectoryIfExists(Path directory) throws IOException {
    Files.walkFileTree(
        directory,
        new SimpleFileVisitor<>() {
          @Override
          public FileVisitResult visitFile(Path file, BasicFileAttributes attributes)
              throws IOException {
            Files.deleteIfExists(file);
            return FileVisitResult.CONTINUE;
          }

          @Override
          public FileVisitResult visitFileFailed(Path file, IOException exception)
              throws IOException {
            if (!Files.exists(file)) {
              return FileVisitResult.CONTINUE;
            }
            throw exception;
          }

          @Override
          public FileVisitResult postVisitDirectory(Path dir, IOException exception)
              throws IOException {
            if (exception != null && Files.exists(dir)) {
              throw exception;
            }
            Files.deleteIfExists(dir);
            return FileVisitResult.CONTINUE;
          }
        });
  }

  private Path rootDirectory() {
    return Path.of(IoTDBDescriptor.getInstance().getConfig().getSortTmpDir(), "device-entry");
  }

  private Path resolveOwnerDirectory(String queryId, String planNodeId) {
    Path queryDirectory = resolveQueryDirectory(queryId);
    Path ownerDirectory = queryDirectory.resolve(planNodeId).normalize();
    return ownerDirectory;
  }

  private Path resolveQueryDirectory(String queryId) {
    Path root = rootDirectory().normalize();
    Path queryDirectory = root.resolve(queryId).normalize();
    return queryDirectory;
  }

  private static class DeviceEntrySpillManagerHolder {
    private static final DeviceEntrySpillManager INSTANCE = new DeviceEntrySpillManager();

    private DeviceEntrySpillManagerHolder() {}
  }
}
