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
import org.apache.iotdb.commons.utils.TestOnly;
import org.apache.iotdb.db.conf.IoTDBDescriptor;

import org.apache.tsfile.external.commons.io.FileUtils;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

public final class DeviceEntrySpillManager {

  private final ConcurrentHashMap<String, Set<Path>> queryDirectories = new ConcurrentHashMap<>();

  private DeviceEntrySpillManager() {}

  public static DeviceEntrySpillManager getInstance() {
    return DeviceEntrySpillManagerHolder.INSTANCE;
  }

  public Path register(String queryId, PlanNodeId planNodeId) throws IOException {
    Path ownerDirectory = rootDirectory().resolve(queryId).resolve(planNodeId.getId());
    Files.createDirectories(ownerDirectory);
    queryDirectories
        .computeIfAbsent(queryId, ignored -> ConcurrentHashMap.newKeySet())
        .add(ownerDirectory);
    return ownerDirectory;
  }

  public void deregisterOwner(String queryId, Path ownerDirectory) throws IOException {
    Set<Path> owners = queryDirectories.get(queryId);
    if (owners != null) {
      owners.remove(ownerDirectory);
      if (owners.isEmpty()) {
        queryDirectories.remove(queryId, owners);
      }
    }
    FileUtils.deleteDirectory(ownerDirectory.toFile());
  }

  public void deregisterQuery(String queryId) throws IOException {
    queryDirectories.remove(queryId);
    FileUtils.deleteDirectory(rootDirectory().resolve(queryId).toFile());
  }

  @TestOnly
  public List<Path> listSegments(String queryId, String planNodeId) throws IOException {
    Path dataSetDirectory = resolveRegisteredDataSetDirectory(queryId, planNodeId);
    try (java.util.stream.Stream<Path> stream = Files.list(dataSetDirectory)) {
      return stream
          .filter(path -> path.getFileName().toString().matches("segment-[0-9]{6,}\\.bin"))
          .sorted(
              Comparator.comparingInt((Path path) -> path.getFileName().toString().length())
                  .thenComparing(path -> path.getFileName().toString()))
          .collect(Collectors.toList());
    }
  }

  public byte[] readSegment(String queryId, String dataSetId, int segmentId) throws IOException {
    return Files.readAllBytes(resolveSegment(queryId, dataSetId, segmentId));
  }

  public Path resolveSegment(String queryId, String dataSetId, int segmentId) throws IOException {
    Path segment = getRegisteredSegmentPath(queryId, dataSetId, segmentId);
    if (!Files.isRegularFile(segment)) {
      throw new java.nio.file.NoSuchFileException(segment.toString());
    }
    return segment;
  }

  public Path resolveSegment(String queryId, PlanNodeId planNodeId, int segmentId)
      throws IOException {
    return resolveSegment(queryId, planNodeId.getId(), segmentId);
  }

  public void deleteSegment(String queryId, String dataSetId, int segmentId) throws IOException {
    Files.deleteIfExists(getRegisteredSegmentPath(queryId, dataSetId, segmentId));
  }

  public void deleteSegment(String queryId, PlanNodeId planNodeId, int segmentId)
      throws IOException {
    deleteSegment(queryId, planNodeId.getId(), segmentId);
  }

  public void finishSegmentDataSet(String queryId, String planNodeId) throws IOException {
    deregisterOwner(queryId, rootDirectory().resolve(queryId).resolve(planNodeId));
  }

  public void deregisterFragment(String queryId, String fragmentInstanceId) throws IOException {
    FileUtils.deleteDirectory(
        resolveUnderRoot(fragmentRootDirectory(), queryId, fragmentInstanceId).toFile());
  }

  public void clearStaleFragmentData() throws IOException {
    FileUtils.deleteDirectory(fragmentRootDirectory().toFile());
    Files.createDirectories(fragmentRootDirectory());
  }

  private Path resolveRegisteredDataSetDirectory(String queryId, String dataSetId)
      throws IOException {
    Path relativeDataSetPath = Path.of(dataSetId);
    if (relativeDataSetPath.isAbsolute()
        || java.util.stream.StreamSupport.stream(relativeDataSetPath.spliterator(), false)
            .anyMatch(path -> path.toString().equals("..") || path.toString().equals("."))) {
      throw new IllegalArgumentException();
    }
    Path queryDirectory = rootDirectory().resolve(queryId).normalize();
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

  private Path getRegisteredSegmentPath(String queryId, String dataSetId, int segmentId)
      throws IOException {
    if (segmentId < 0) {
      throw new IllegalArgumentException();
    }
    return resolveRegisteredDataSetDirectory(queryId, dataSetId)
        .resolve(String.format("segment-%06d.bin", segmentId));
  }

  public void clearStaleData() throws IOException {
    FileUtils.deleteDirectory(rootDirectory().toFile());
    Files.createDirectories(rootDirectory());
    queryDirectories.clear();
  }

  private Path rootDirectory() {
    return Path.of(IoTDBDescriptor.getInstance().getConfig().getSortTmpDir(), "device-entry");
  }

  private Path fragmentRootDirectory() {
    return rootDirectory().resolve("fragment");
  }

  private Path resolveUnderRoot(Path root, String... children) {
    Path result = root;
    for (String child : children) {
      result = result.resolve(child);
    }
    result = result.normalize();
    if (!result.startsWith(root.normalize())) {
      throw new IllegalArgumentException();
    }
    return result;
  }

  private static class DeviceEntrySpillManagerHolder {
    private static final DeviceEntrySpillManager INSTANCE = new DeviceEntrySpillManager();

    private DeviceEntrySpillManagerHolder() {}
  }
}
