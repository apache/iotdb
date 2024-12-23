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

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Map;
import java.util.Objects;
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

  public void addPipeMeta(String pipeName, PipeMeta pipeMeta) {
    pipeNameToPipeMetaMap.put(pipeName, pipeMeta);
  }

  public PipeMeta getPipeMeta(String pipeName) {
    return pipeNameToPipeMetaMap.get(pipeName);
  }

  public void removePipeMeta(String pipeName) {
    pipeNameToPipeMetaMap.remove(pipeName);
  }

  public boolean containsPipeMeta(String pipeName) {
    return pipeNameToPipeMetaMap.containsKey(pipeName);
  }

  public Iterable<PipeMeta> getPipeMetaList() {
    return pipeNameToPipeMetaMap.values();
  }

  public int getPipeMetaCount() {
    return pipeNameToPipeMetaMap.size();
  }

  public PipeMeta getPipeMetaByPipeName(String pipeName) {
    return pipeNameToPipeMetaMap.get(pipeName);
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
      ReadWriteIOUtils.write(entry.getKey(), fileOutputStream);
      entry.getValue().serialize(fileOutputStream);
    }
  }

  public void processLoadSnapshot(FileInputStream fileInputStream) throws IOException {
    clear();

    final int size = ReadWriteIOUtils.readInt(fileInputStream);
    for (int i = 0; i < size; i++) {
      final String pipeName = ReadWriteIOUtils.readString(fileInputStream);
      pipeNameToPipeMetaMap.put(pipeName, PipeMeta.deserialize(fileInputStream));
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
}
