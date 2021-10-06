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
package org.apache.iotdb.db.metadata.metadisk.metafile;

import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.mnode.IMNode;

import java.io.IOException;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MockMetaFile implements IMetaFileAccess {

  private final Map<Long, IMNode> positionFile = new ConcurrentHashMap<>();
  private final long rootPosition = 0;
  private long freePosition = 1;

  public MockMetaFile(String metaFilePath) {}

  @Override
  public IMNode readRoot() throws IOException {
    return positionFile.get(rootPosition);
  }

  @Override
  public IMNode read(IPersistenceInfo persistenceInfo) throws IOException {
    return positionFile.get(persistenceInfo.getStartPosition());
  }

  @Override
  public void write(IMNode mNode) throws IOException {
    if (!mNode.isPersisted()) {
      if (mNode.getFullPath().equals("root")) {
        mNode.setPersistenceInfo(IPersistenceInfo.createPersistenceInfo(rootPosition));
      } else {
        mNode.setPersistenceInfo(IPersistenceInfo.createPersistenceInfo(freePosition++));
      }
    }
    IMNode persistedMNode = mNode.clone();
    persistedMNode.setCacheEntry(null);
    for (String childName : persistedMNode.getChildren().keySet()) {
      // require child be written before parent
      persistedMNode.evictChild(childName);
    }
    positionFile.put(persistedMNode.getPersistenceInfo().getStartPosition(), persistedMNode);
  }

  @Override
  public void write(Collection<IMNode> mNodes) throws IOException {
    for (IMNode mNode : mNodes) {
      write(mNode);
    }
  }

  @Override
  public void remove(IPersistenceInfo persistenceInfo) throws IOException {
    positionFile.remove(persistenceInfo.getStartPosition());
  }

  @Override
  public void close() throws IOException {}

  @Override
  public void sync() throws IOException {}

  public IMNode read(PartialPath path) throws IOException {
    return getMNodeByPath(path);
  }

  public void remove(PartialPath path) throws IOException {
    positionFile.remove(getMNodeByPath(path).getPersistenceInfo().getStartPosition());
  }

  // todo require all node in the path be persist, which means when write child, the parent should
  // also be written
  private IMNode getMNodeByPath(PartialPath path) {
    String[] nodes = path.getNodes();
    IMNode cur = positionFile.get(rootPosition);
    for (int i = 1; i < nodes.length; i++) {
      cur = positionFile.get(cur.getChild(nodes[i]).getPersistenceInfo().getStartPosition());
    }
    return cur;
  }
}
