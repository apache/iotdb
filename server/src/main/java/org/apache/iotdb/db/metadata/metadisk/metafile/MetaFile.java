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
import java.util.*;

public class MetaFile implements IMetaFileAccess {

  private final MTreeFile mTreeFile;

  public MetaFile(String mTreeFilePath) throws IOException {
    mTreeFile = new MTreeFile(mTreeFilePath);
  }

  @Override
  public IMNode readRoot() throws IOException {
    return mTreeFile.read(IPersistenceInfo.createPersistenceInfo(mTreeFile.getRootPosition()));
  }

  @Override
  public IMNode read(IPersistenceInfo persistenceInfo) throws IOException {
    return mTreeFile.read(persistenceInfo);
  }

  @Override
  public void write(IMNode mNode) throws IOException {
    mTreeFile.write(mNode);
  }

  @Override
  public void write(Collection<IMNode> mNodes) throws IOException {
    allocateFreePos(mNodes);
    for (IMNode mNode : mNodes) {
      write(mNode);
    }
  }

  @Override
  public void remove(IPersistenceInfo persistenceInfo) throws IOException {
    mTreeFile.remove(persistenceInfo);
  }

  @Override
  public void close() throws IOException {
    mTreeFile.close();
  }

  @Override
  public void sync() throws IOException {
    mTreeFile.sync();
  }

  public IMNode read(String path) throws IOException {
    return mTreeFile.read(path);
  }

  public IMNode readRecursively(IPersistenceInfo persistenceInfo) throws IOException {
    return mTreeFile.readRecursively(persistenceInfo);
  }

  public void writeRecursively(IMNode mNode) throws IOException {
    List<IMNode> mNodeList = new LinkedList<>();
    flatten(mNode, mNodeList);
    write(mNodeList);
  }

  public void remove(PartialPath path) throws IOException {}

  private void flatten(IMNode mNode, Collection<IMNode> mNodes) {
    mNodes.add(mNode);
    for (IMNode child : mNode.getChildren().values()) {
      flatten(child, mNodes);
    }
  }

  private void allocateFreePos(Collection<IMNode> mNodes) throws IOException {
    for (IMNode mNode : mNodes) {
      if (mNode.getPersistenceInfo() != null) {
        continue;
      }
      if (mNode.getName().equals("root")) {
        mNode.setPersistenceInfo(
            IPersistenceInfo.createPersistenceInfo(mTreeFile.getRootPosition()));
      } else {
        mNode.setPersistenceInfo(IPersistenceInfo.createPersistenceInfo(mTreeFile.getFreePos()));
      }
    }
  }
}
