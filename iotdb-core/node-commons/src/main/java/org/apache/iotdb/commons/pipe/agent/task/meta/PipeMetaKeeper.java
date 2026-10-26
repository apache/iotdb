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

package org.apache.iotdb.commons.pipe.agent.task.meta;

import org.apache.iotdb.commons.pipe.datastructure.visibility.Visibility;
import org.apache.iotdb.commons.pipe.datastructure.visibility.VisibilityUtils;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public class PipeMetaKeeper {

  protected final Map<String, PipeMeta> pipeNameToPipeMetaMap;

  private final ReentrantReadWriteLock pipeMetaKeeperLock;

  public PipeMetaKeeper() {
    pipeNameToPipeMetaMap = new ConcurrentHashMap<>();
    pipeMetaKeeperLock = new ReentrantReadWriteLock(true);
  }

  /////////////////////////////////  Lock  /////////////////////////////////

  public void acquireReadLock() {
    pipeMetaKeeperLock.readLock().lock();
  }

  public boolean tryReadLock(long timeOut) throws InterruptedException {
    return pipeMetaKeeperLock.readLock().tryLock(timeOut, TimeUnit.SECONDS);
  }

  public void releaseReadLock() {
    pipeMetaKeeperLock.readLock().unlock();
  }

  public void acquireWriteLock() {
    pipeMetaKeeperLock.writeLock().lock();
  }

  public boolean tryWriteLock(long timeOut) throws InterruptedException {
    return pipeMetaKeeperLock.writeLock().tryLock(timeOut, TimeUnit.SECONDS);
  }

  public void releaseWriteLock() {
    pipeMetaKeeperLock.writeLock().unlock();
  }

  /////////////////////////////////  PipeMeta  /////////////////////////////////

  public void addPipeMeta(PipeMeta pipeMeta) {
    pipeNameToPipeMetaMap.put(generatePipeMetaKeeperKey(pipeMeta.getStaticMeta()), pipeMeta);
  }

  public PipeMeta getPipeMeta(String pipeName) {
    return getPipeMetaOptional(pipeName).orElse(null);
  }

  public PipeMeta getPipeMeta(String pipeName, boolean isTableModel) {
    return getPipeMetaOptional(pipeName, isTableModel).orElse(null);
  }

  public PipeMeta getPipeMeta(String pipeName, boolean isTableModelSet, boolean isTableModel) {
    return isTableModelSet ? getPipeMeta(pipeName, isTableModel) : getPipeMeta(pipeName);
  }

  public PipeMeta getPipeMeta(PipeStaticMeta pipeStaticMeta) {
    return pipeNameToPipeMetaMap.get(generatePipeMetaKeeperKey(pipeStaticMeta));
  }

  public PipeMeta getOverlappedPipeMeta(PipeStaticMeta pipeStaticMeta) {
    return pipeNameToPipeMetaMap.values().stream()
        .filter(
            pipeMeta ->
                pipeMeta.getStaticMeta().getPipeName().equals(pipeStaticMeta.getPipeName())
                    && isVisibilityOverlapped(pipeMeta.getStaticMeta(), pipeStaticMeta))
        .findFirst()
        .orElse(null);
  }

  public PipeMeta getPipeMeta(String pipeName, long creationTime) {
    return getPipeMetaOptional(pipeName, creationTime).orElse(null);
  }

  public void removePipeMeta(String pipeName) {
    pipeMetaKeeperKeyOptional(pipeName).ifPresent(pipeNameToPipeMetaMap::remove);
  }

  public void removePipeMeta(String pipeName, boolean isTableModel) {
    pipeMetaKeeperKeyOptional(pipeName, isTableModel).ifPresent(pipeNameToPipeMetaMap::remove);
  }

  public void removePipeMeta(PipeStaticMeta pipeStaticMeta) {
    pipeNameToPipeMetaMap.remove(generatePipeMetaKeeperKey(pipeStaticMeta));
  }

  public boolean containsPipeMeta(String pipeName) {
    return pipeMetaKeeperKeyOptional(pipeName).isPresent();
  }

  public boolean containsPipeMeta(String pipeName, boolean isTableModel) {
    return pipeMetaKeeperKeyOptional(pipeName, isTableModel).isPresent();
  }

  public boolean containsPipeMeta(PipeStaticMeta pipeStaticMeta) {
    return pipeNameToPipeMetaMap.containsKey(generatePipeMetaKeeperKey(pipeStaticMeta));
  }

  public boolean containsPipeMetaOverlapped(PipeStaticMeta pipeStaticMeta) {
    return containsPipeMetaOverlapped(pipeStaticMeta, null);
  }

  public boolean containsPipeMetaOverlapped(
      PipeStaticMeta pipeStaticMeta, PipeStaticMeta excludedPipeStaticMeta) {
    return pipeNameToPipeMetaMap.values().stream()
        .anyMatch(
            pipeMeta ->
                pipeMeta.getStaticMeta().getPipeName().equals(pipeStaticMeta.getPipeName())
                    && !pipeMeta.getStaticMeta().equals(excludedPipeStaticMeta)
                    && isVisibilityOverlapped(pipeMeta.getStaticMeta(), pipeStaticMeta));
  }

  public Iterable<PipeMeta> getPipeMetaList() {
    return pipeNameToPipeMetaMap.values();
  }

  public int getPipeMetaCount() {
    return pipeNameToPipeMetaMap.size();
  }

  public PipeMeta getPipeMetaByPipeName(String pipeName) {
    return getPipeMeta(pipeName);
  }

  public PipeMeta getPipeMetaByPipeName(String pipeName, boolean isTableModel) {
    return getPipeMeta(pipeName, isTableModel);
  }

  public void clear() {
    this.pipeNameToPipeMetaMap.clear();
  }

  public boolean isEmpty() {
    return pipeNameToPipeMetaMap.isEmpty();
  }

  /////////////////////////////////  Snapshot  /////////////////////////////////

  public void processTakeSnapshot(FileOutputStream fileOutputStream) throws IOException {
    ReadWriteIOUtils.write(pipeNameToPipeMetaMap.size(), fileOutputStream);
    for (Map.Entry<String, PipeMeta> entry : pipeNameToPipeMetaMap.entrySet()) {
      ReadWriteIOUtils.write(entry.getValue().getStaticMeta().getPipeName(), fileOutputStream);
      entry.getValue().serialize(fileOutputStream);
    }
  }

  public void processLoadSnapshot(FileInputStream fileInputStream) throws IOException {
    clear();

    final int size = ReadWriteIOUtils.readInt(fileInputStream);
    for (int i = 0; i < size; i++) {
      ReadWriteIOUtils.readString(fileInputStream);
      final PipeMeta pipeMeta = PipeMeta.deserialize(fileInputStream);
      pipeNameToPipeMetaMap.put(generatePipeMetaKeeperKey(pipeMeta.getStaticMeta()), pipeMeta);
    }
  }

  /////////////////////////////////  Override  /////////////////////////////////

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    PipeMetaKeeper that = (PipeMetaKeeper) o;
    return Objects.equals(pipeNameToPipeMetaMap, that.pipeNameToPipeMetaMap);
  }

  @Override
  public int hashCode() {
    return Objects.hash(pipeNameToPipeMetaMap);
  }

  @Override
  public String toString() {
    return "PipeMetaKeeper{" + "pipeNameToPipeMetaMap=" + pipeNameToPipeMetaMap + '}';
  }

  //////////////////////////// APIs provided for metric framework ////////////////////////////

  public long runningPipeCount() {
    return pipeNameToPipeMetaMap.values().stream()
        .filter(pipeMeta -> PipeStatus.RUNNING.equals(pipeMeta.getRuntimeMeta().getStatus().get()))
        .count();
  }

  public long droppedPipeCount() {
    return pipeNameToPipeMetaMap.values().stream()
        .filter(pipeMeta -> PipeStatus.DROPPED.equals(pipeMeta.getRuntimeMeta().getStatus().get()))
        .count();
  }

  public long userStoppedPipeCount() {
    return pipeNameToPipeMetaMap.values().stream()
        .filter(
            pipeMeta ->
                PipeStatus.STOPPED.equals(pipeMeta.getRuntimeMeta().getStatus().get())
                    && !pipeMeta.getRuntimeMeta().getIsStoppedByRuntimeException())
        .count();
  }

  public long exceptionStoppedPipeCount() {
    return pipeNameToPipeMetaMap.values().stream()
        .filter(
            pipeMeta ->
                PipeStatus.STOPPED.equals(pipeMeta.getRuntimeMeta().getStatus().get())
                    && pipeMeta.getRuntimeMeta().getIsStoppedByRuntimeException())
        .count();
  }

  /////////////////////////////////  Tree & Table Isolation  /////////////////////////////////

  private Optional<PipeMeta> getPipeMetaOptional(String pipeName) {
    return pipeMetaKeeperKeyOptional(pipeName).map(pipeNameToPipeMetaMap::get);
  }

  private Optional<PipeMeta> getPipeMetaOptional(String pipeName, boolean isTableModel) {
    return pipeMetaKeeperKeyOptional(pipeName, isTableModel).map(pipeNameToPipeMetaMap::get);
  }

  private Optional<PipeMeta> getPipeMetaOptional(String pipeName, long creationTime) {
    final PipeMeta pipeMeta = pipeNameToPipeMetaMap.get(pipeName);
    if (Objects.nonNull(pipeMeta) && pipeMeta.getStaticMeta().getCreationTime() == creationTime) {
      return Optional.of(pipeMeta);
    }

    return pipeNameToPipeMetaMap.values().stream()
        .filter(
            meta ->
                meta.getStaticMeta().getPipeName().equals(pipeName)
                    && meta.getStaticMeta().getCreationTime() == creationTime)
        .findFirst();
  }

  private Optional<String> pipeMetaKeeperKeyOptional(String pipeName, boolean isTableModel) {
    final String modelKey = generatePipeMetaKeeperKey(pipeName, isTableModel);
    if (pipeNameToPipeMetaMap.containsKey(modelKey)) {
      return Optional.of(modelKey);
    }

    final String bothModelKey = generatePipeMetaKeeperKeyForVisibility(pipeName, Visibility.BOTH);
    return pipeNameToPipeMetaMap.containsKey(bothModelKey)
        ? Optional.of(bothModelKey)
        : Optional.empty();
  }

  private Optional<String> pipeMetaKeeperKeyOptional(String pipeName) {
    final String treeModelKey = generatePipeMetaKeeperKey(pipeName, false);
    if (pipeNameToPipeMetaMap.containsKey(treeModelKey)) {
      return Optional.of(treeModelKey);
    }

    final String bothModelKey = generatePipeMetaKeeperKeyForVisibility(pipeName, Visibility.BOTH);
    if (pipeNameToPipeMetaMap.containsKey(bothModelKey)) {
      return Optional.of(bothModelKey);
    }

    final String tableModelKey = generatePipeMetaKeeperKey(pipeName, true);
    if (pipeNameToPipeMetaMap.containsKey(tableModelKey)) {
      return Optional.of(tableModelKey);
    }

    final String noneModelKey = generatePipeMetaKeeperKeyForVisibility(pipeName, Visibility.NONE);
    return pipeNameToPipeMetaMap.containsKey(noneModelKey)
        ? Optional.of(noneModelKey)
        : Optional.empty();
  }

  private static String generatePipeMetaKeeperKey(final PipeStaticMeta pipeStaticMeta) {
    final String pipeName = pipeStaticMeta.getPipeName();
    final Visibility visibility =
        VisibilityUtils.calculateFromExtractorParameters(pipeStaticMeta.getSourceParameters());
    if (Objects.equals(Visibility.TREE_ONLY, visibility)) {
      return generatePipeMetaKeeperKey(pipeName, false);
    }
    if (Objects.equals(Visibility.TABLE_ONLY, visibility)) {
      return generatePipeMetaKeeperKey(pipeName, true);
    }
    if (Objects.equals(Visibility.NONE, visibility)) {
      return generatePipeMetaKeeperKeyForVisibility(pipeName, Visibility.NONE);
    }
    return generatePipeMetaKeeperKeyForVisibility(pipeName, Visibility.BOTH);
  }

  private static String generatePipeMetaKeeperKey(
      final String pipeName, final boolean isTableModel) {
    return (isTableModel ? "table:" : "tree:") + pipeName;
  }

  private static String generatePipeMetaKeeperKeyForVisibility(
      final String pipeName, final Visibility visibility) {
    return visibility.name().toLowerCase() + ":" + pipeName;
  }

  private static boolean isVisibilityOverlapped(
      final PipeStaticMeta leftPipeStaticMeta, final PipeStaticMeta rightPipeStaticMeta) {
    final Visibility leftVisibility =
        VisibilityUtils.calculateFromExtractorParameters(leftPipeStaticMeta.getSourceParameters());
    final Visibility rightVisibility =
        VisibilityUtils.calculateFromExtractorParameters(rightPipeStaticMeta.getSourceParameters());
    return VisibilityUtils.isCompatible(leftVisibility, rightVisibility)
        || VisibilityUtils.isCompatible(rightVisibility, leftVisibility);
  }
}
