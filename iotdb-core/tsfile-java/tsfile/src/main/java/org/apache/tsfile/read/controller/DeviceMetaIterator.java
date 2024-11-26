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
package org.apache.tsfile.read.controller;

import org.apache.tsfile.file.IMetadataIndexEntry;
import org.apache.tsfile.file.metadata.DeviceMetadataIndexEntry;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.MetadataIndexNode;
import org.apache.tsfile.file.metadata.enums.MetadataIndexNodeType;
import org.apache.tsfile.read.TsFileSequenceReader;
import org.apache.tsfile.read.expression.ExpressionTree;
import org.apache.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Queue;

public class DeviceMetaIterator implements Iterator<Pair<IDeviceID, MetadataIndexNode>> {

  private static final Logger LOGGER = LoggerFactory.getLogger(DeviceMetaIterator.class);
  private final TsFileSequenceReader tsFileSequenceReader;
  private final Queue<MetadataIndexNode> metadataIndexNodes = new ArrayDeque<>();
  private final Queue<Pair<IDeviceID, MetadataIndexNode>> resultCache = new ArrayDeque<>();
  private final ExpressionTree idFilter;

  public DeviceMetaIterator(
      TsFileSequenceReader tsFileSequenceReader,
      MetadataIndexNode metadataIndexNode,
      ExpressionTree idFilter) {
    this.tsFileSequenceReader = tsFileSequenceReader;
    this.metadataIndexNodes.add(metadataIndexNode);
    this.idFilter = idFilter;
  }

  @Override
  public boolean hasNext() {
    if (!resultCache.isEmpty()) {
      return true;
    }
    try {
      loadResults();
    } catch (IOException e) {
      LOGGER.error("Failed to load device meta data", e);
      return false;
    }

    return !resultCache.isEmpty();
  }

  private void loadLeafDevice(MetadataIndexNode currentNode) throws IOException {
    List<IMetadataIndexEntry> leafChildren = currentNode.getChildren();
    for (int i = 0; i < leafChildren.size(); i++) {
      IMetadataIndexEntry child = leafChildren.get(i);
      final IDeviceID deviceID = ((DeviceMetadataIndexEntry) child).getDeviceID();
      if (idFilter != null && !idFilter.satisfy(deviceID)) {
        continue;
      }

      long startOffset = child.getOffset();
      long endOffset =
          i < leafChildren.size() - 1
              ? leafChildren.get(i + 1).getOffset()
              : currentNode.getEndOffset();
      final MetadataIndexNode childNode =
          tsFileSequenceReader.readMetadataIndexNode(startOffset, endOffset, false);
      resultCache.add(new Pair<>(deviceID, childNode));
    }
  }

  private void loadInternalNode(MetadataIndexNode currentNode) throws IOException {
    List<IMetadataIndexEntry> internalChildren = currentNode.getChildren();
    for (int i = 0; i < internalChildren.size(); i++) {
      IMetadataIndexEntry child = internalChildren.get(i);
      long startOffset = child.getOffset();
      long endOffset =
          i < internalChildren.size() - 1
              ? internalChildren.get(i + 1).getOffset()
              : currentNode.getEndOffset();
      final MetadataIndexNode childNode =
          tsFileSequenceReader.readMetadataIndexNode(startOffset, endOffset, true);
      metadataIndexNodes.add(childNode);
    }
  }

  private void loadResults() throws IOException {
    while (!metadataIndexNodes.isEmpty()) {
      final MetadataIndexNode currentNode = metadataIndexNodes.poll();
      final MetadataIndexNodeType nodeType = currentNode.getNodeType();
      switch (nodeType) {
        case LEAF_DEVICE:
          loadLeafDevice(currentNode);
          if (!resultCache.isEmpty()) {
            return;
          }
        case INTERNAL_DEVICE:
          loadInternalNode(currentNode);
          break;
        default:
          throw new IOException("A non-device node detected: " + currentNode);
      }
    }
  }

  @Override
  public Pair<IDeviceID, MetadataIndexNode> next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return resultCache.poll();
  }
}
