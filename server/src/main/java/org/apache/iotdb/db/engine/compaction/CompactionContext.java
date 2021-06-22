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

package org.apache.iotdb.db.engine.compaction;

import org.apache.iotdb.db.engine.compaction.cross.inplace.manage.CrossSpaceMergeResource;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.engine.storagegroup.TsFileResourceList;

import java.io.File;
import java.util.List;

public class CompactionContext {
  protected String storageGroupName;
  protected String virtualStorageGroupName;
  protected String storageGroupDir;
  protected long timePartitionId;
  protected boolean sequence;
  protected TsFileResourceList sequenceFileResourceList;
  protected TsFileResourceList unsequenceFileResourceList;
  protected List<TsFileResource> selectedSequenceFiles;
  protected List<TsFileResource> selectedUnsequenceFiles;
  protected ModificationFile compactionModification;
  protected CrossSpaceMergeResource mergeResource;
  protected int concurrentMergeCount;
  protected File compactionLogFile;
  protected List<TsFileResource> recoverTsFileList;

  public CompactionContext() {}

  public String getStorageGroupName() {
    return storageGroupName;
  }

  public void setStorageGroupName(String storageGroupName) {
    this.storageGroupName = storageGroupName;
  }

  public long getTimePartitionId() {
    return timePartitionId;
  }

  public void setTimePartitionId(long timePartitionId) {
    this.timePartitionId = timePartitionId;
  }

  public boolean isSequence() {
    return sequence;
  }

  public void setSequence(boolean sequence) {
    this.sequence = sequence;
  }

  public TsFileResourceList getSequenceFileResourceList() {
    return sequenceFileResourceList;
  }

  public void setSequenceFileResourceList(TsFileResourceList sequenceFileResourceList) {
    this.sequenceFileResourceList = sequenceFileResourceList;
  }

  public TsFileResourceList getUnsequenceFileResourceList() {
    return unsequenceFileResourceList;
  }

  public void setUnsequenceFileResourceList(TsFileResourceList unsequenceFileResourceList) {
    this.unsequenceFileResourceList = unsequenceFileResourceList;
  }

  public List<TsFileResource> getSelectedSequenceFiles() {
    return selectedSequenceFiles;
  }

  public void setSelectedSequenceFiles(List<TsFileResource> selectedSequenceFiles) {
    this.selectedSequenceFiles = selectedSequenceFiles;
  }

  public List<TsFileResource> getSelectedUnsequenceFiles() {
    return selectedUnsequenceFiles;
  }

  public void setSelectedUnsequenceFiles(List<TsFileResource> selectedUnsequenceFiles) {
    this.selectedUnsequenceFiles = selectedUnsequenceFiles;
  }

  public ModificationFile getCompactionModification() {
    return compactionModification;
  }

  public void setCompactionModification(ModificationFile compactionModification) {
    this.compactionModification = compactionModification;
  }

  public CrossSpaceMergeResource getMergeResource() {
    return mergeResource;
  }

  public void setMergeResource(CrossSpaceMergeResource mergeResource) {
    this.mergeResource = mergeResource;
  }

  public int getConcurrentMergeCount() {
    return concurrentMergeCount;
  }

  public void setConcurrentMergeCount(int concurrentMergeCount) {
    this.concurrentMergeCount = concurrentMergeCount;
  }

  public String getStorageGroupDir() {
    return storageGroupDir;
  }

  public void setStorageGroupDir(String storageGroupDir) {
    this.storageGroupDir = storageGroupDir;
  }

  public String getVirtualStorageGroupName() {
    return virtualStorageGroupName;
  }

  public void setVirtualStorageGroupName(String virtualStorageGroupName) {
    this.virtualStorageGroupName = virtualStorageGroupName;
  }

  public File getCompactionLogFile() {
    return compactionLogFile;
  }

  public void setCompactionLogFile(File compactionLogFile) {
    this.compactionLogFile = compactionLogFile;
  }

  public List<TsFileResource> getRecoverTsFileList() {
    return recoverTsFileList;
  }

  public void setRecoverTsFileList(List<TsFileResource> recoverTsFileList) {
    this.recoverTsFileList = recoverTsFileList;
  }
}
