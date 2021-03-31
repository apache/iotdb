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

package org.apache.iotdb.db.engine.compaction.no;

import org.apache.iotdb.db.engine.compaction.TsFileManagement;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

public class NoCompactionTsFileManagement extends TsFileManagement {

  private static final Logger logger = LoggerFactory.getLogger(NoCompactionTsFileManagement.class);
  // includes sealed and unsealed sequence TsFiles
  private final Map<Long, TreeSet<TsFileResource>> sequenceFileTreeSetMap = new TreeMap<>();

  // includes sealed and unsealed unSequence TsFiles
  private final Map<Long, List<TsFileResource>> unSequenceFileListMap = new TreeMap<>();

  public NoCompactionTsFileManagement(String storageGroupName, String storageGroupDir) {
    super(storageGroupName, storageGroupDir);
  }

  @Deprecated
  @Override
  public List<TsFileResource> getTsFileList(boolean sequence) {
    List<TsFileResource> result = new ArrayList<>();
    if (sequence) {
      synchronized (sequenceFileTreeSetMap) {
        for (TreeSet<TsFileResource> tsFileResourceTreeSet : sequenceFileTreeSetMap.values()) {
          result.addAll(tsFileResourceTreeSet);
        }
      }
    } else {
      synchronized (unSequenceFileListMap) {
        for (List<TsFileResource> tsFileResourceList : unSequenceFileListMap.values()) {
          result.addAll(tsFileResourceList);
        }
      }
    }
    return result;
  }

  @Override
  public List<TsFileResource> getTsFileListByTimePartition(boolean sequence, long timePartition) {
    if (sequence) {
      synchronized (sequenceFileTreeSetMap) {
        return new ArrayList<>(sequenceFileTreeSetMap.getOrDefault(timePartition, new TreeSet<>()));
      }
    } else {
      synchronized (unSequenceFileListMap) {
        return new ArrayList<>(
            unSequenceFileListMap.getOrDefault(timePartition, Collections.emptyList()));
      }
    }
  }

  @Override
  public Iterator<TsFileResource> getIterator(boolean sequence) {
    return getTsFileList(sequence).iterator();
  }

  @Override
  public void remove(TsFileResource tsFileResource, boolean sequence) {
    if (sequence) {
      synchronized (sequenceFileTreeSetMap) {
        TreeSet<TsFileResource> sequenceFileTreeSet =
            sequenceFileTreeSetMap.get(tsFileResource.getTimePartition());
        sequenceFileTreeSet.remove(tsFileResource);
      }
    } else {
      synchronized (unSequenceFileListMap) {
        List<TsFileResource> unSequenceFileList =
            unSequenceFileListMap.get(tsFileResource.getTimePartition());
        unSequenceFileList.remove(tsFileResource);
      }
    }
  }

  @Override
  public void removeAll(List<TsFileResource> tsFileResourceList, boolean sequence) {
    if (tsFileResourceList.size() > 0) {
      tsFileResourceList.sort((o1, o2) -> (int) (o1.getTimePartition() - o2.getTimePartition()));
      if (sequence) {
        synchronized (sequenceFileTreeSetMap) {
          long currTimePartition = tsFileResourceList.get(0).getTimePartition();
          int startIndex = 0;
          for (int i = 1; i < tsFileResourceList.size(); i++) {
            TsFileResource tsFileResource = tsFileResourceList.get(i);
            if (tsFileResource.getTimePartition() != currTimePartition) {
              sequenceFileTreeSetMap
                  .get(currTimePartition)
                  .removeAll(tsFileResourceList.subList(startIndex, i));
              currTimePartition = tsFileResource.getTimePartition();
              startIndex = i;
            }
          }
          sequenceFileTreeSetMap
              .get(currTimePartition)
              .removeAll(tsFileResourceList.subList(startIndex, tsFileResourceList.size()));
        }
      } else {
        synchronized (unSequenceFileListMap) {
          long currTimePartition = tsFileResourceList.get(0).getTimePartition();
          int startIndex = 0;
          for (int i = 1; i < tsFileResourceList.size(); i++) {
            TsFileResource tsFileResource = tsFileResourceList.get(i);
            if (tsFileResource.getTimePartition() != currTimePartition) {
              unSequenceFileListMap
                  .get(currTimePartition)
                  .removeAll(tsFileResourceList.subList(startIndex, i));
              currTimePartition = tsFileResource.getTimePartition();
              startIndex = i;
            }
          }
          unSequenceFileListMap
              .get(currTimePartition)
              .removeAll(tsFileResourceList.subList(startIndex, tsFileResourceList.size()));
        }
      }
    }
  }

  @Override
  public void add(TsFileResource tsFileResource, boolean sequence) {
    long timePartitionId = tsFileResource.getTimePartition();
    if (sequence) {
      synchronized (sequenceFileTreeSetMap) {
        sequenceFileTreeSetMap
            .computeIfAbsent(timePartitionId, this::newSequenceTsFileResources)
            .add(tsFileResource);
      }
    } else {
      synchronized (unSequenceFileListMap) {
        unSequenceFileListMap
            .computeIfAbsent(timePartitionId, this::newUnSequenceTsFileResources)
            .add(tsFileResource);
      }
    }
  }

  @Override
  public void addRecover(TsFileResource tsFileResource, boolean sequence) {
    logger.info("{} do not need to recover", storageGroupName);
  }

  @Override
  public void addAll(List<TsFileResource> tsFileResourceList, boolean sequence) {
    for (TsFileResource tsFileResource : tsFileResourceList) {
      add(tsFileResource, sequence);
    }
  }

  @Override
  public boolean contains(TsFileResource tsFileResource, boolean sequence) {
    if (sequence) {
      synchronized (sequenceFileTreeSetMap) {
        return sequenceFileTreeSetMap
            .getOrDefault(tsFileResource.getTimePartition(), newSequenceTsFileResources(0L))
            .contains(tsFileResource);
      }
    } else {
      synchronized (unSequenceFileListMap) {
        return unSequenceFileListMap
            .getOrDefault(tsFileResource.getTimePartition(), new ArrayList<>())
            .contains(tsFileResource);
      }
    }
  }

  @Override
  public void clear() {
    sequenceFileTreeSetMap.clear();
    unSequenceFileListMap.clear();
  }

  @Override
  public boolean isEmpty(boolean sequence) {
    if (sequence) {
      synchronized (sequenceFileTreeSetMap) {
        for (Set<TsFileResource> sequenceFileTreeSet : sequenceFileTreeSetMap.values()) {
          if (!sequenceFileTreeSet.isEmpty()) {
            return false;
          }
        }
      }
    } else {
      synchronized (unSequenceFileListMap) {
        for (List<TsFileResource> unSequenceFileList : unSequenceFileListMap.values()) {
          if (!unSequenceFileList.isEmpty()) {
            return false;
          }
        }
      }
    }
    return true;
  }

  @Override
  public int size(boolean sequence) {
    int result = 0;
    if (sequence) {
      synchronized (sequenceFileTreeSetMap) {
        for (Set<TsFileResource> sequenceFileTreeSet : sequenceFileTreeSetMap.values()) {
          result += sequenceFileTreeSet.size();
        }
      }
    } else {
      synchronized (unSequenceFileListMap) {
        for (List<TsFileResource> unSequenceFileList : unSequenceFileListMap.values()) {
          result += unSequenceFileList.size();
        }
      }
    }
    return result;
  }

  @Override
  public void recover() {
    logger.info("{} no recover logic", storageGroupName);
  }

  @Override
  public void forkCurrentFileList(long timePartition) {
    logger.info("{} do not need fork", storageGroupName);
  }

  @Override
  protected void merge(long timePartition) {
    logger.info("{} no merge logic", storageGroupName);
  }

  private TreeSet<TsFileResource> newSequenceTsFileResources(Long k) {
    return new TreeSet<>((o1, o2) -> compareFileName(o1.getTsFile(), o2.getTsFile()));
  }

  private List<TsFileResource> newUnSequenceTsFileResources(Long k) {
    return new ArrayList<>();
  }
}
