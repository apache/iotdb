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

package org.apache.iotdb.tsfile.file.metadata;

import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.utils.ReadWriteForEncodingUtils;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

public class MetadataIndexBucket {

  private final List<MetadataIndexBucketEntry> children;

  public MetadataIndexBucket() {
    this.children = new ArrayList<>();
  }

  public MetadataIndexBucket(List<MetadataIndexBucketEntry> children) {
    this.children = children;
  }

  public List<MetadataIndexBucketEntry> getChildren() {
    return children;
  }

  public void addEntry(MetadataIndexBucketEntry entry) {
    this.children.add(entry);
  }

  public void orderEntries() {
    this.children.sort(Comparator.comparing(MetadataIndexBucketEntry::getPath));
  }

  public int serializeTo(ByteBuffer buffer) throws IOException {
    int byteLen = 0;
    byteLen += ReadWriteForEncodingUtils.writeUnsignedVarInt(children.size(), buffer);
    for (MetadataIndexBucketEntry entry : children) {
      byteLen += entry.serializeTo(buffer);
    }
    return byteLen;
  }

  public static MetadataIndexBucket deserializeFrom(ByteBuffer buffer) {
    List<MetadataIndexBucketEntry> children = new ArrayList<>();
    int size = ReadWriteForEncodingUtils.readUnsignedVarInt(buffer);
    for (int i = 0; i < size; i++) {
      children.add(MetadataIndexBucketEntry.deserializeFrom(buffer));
    }
    return new MetadataIndexBucket(children);
  }

  /**
   * get startOffset and endOffset of the TimeseriesMetadata
   *
   * @param key path
   */
  public Pair<Long, Integer> getChildIndexEntry(String key) {
    int index = binarySearchInChildren(key);
    if (index == -1) {
      return null;
    }
    return new Pair<>(children.get(index).getOffset(), children.get(index).getSize());
  }

  int binarySearchInChildren(String key) {
    int low = 0;
    int high = children.size() - 1;

    while (low <= high) {
      int mid = (low + high) >>> 1;
      MetadataIndexBucketEntry midVal = children.get(mid);
      int cmp = midVal.getPath().compareTo(key);

      if (cmp < 0) {
        low = mid + 1;
      } else if (cmp > 0) {
        high = mid - 1;
      } else {
        return mid; // key found
      }
    }

    // key not found
    return -1;
  }

  public int getSerializeSize() {
    int size = ReadWriteForEncodingUtils.uVarIntSize(children.size());
    for (MetadataIndexBucketEntry entry : children) {
      size += entry.getSerializeSize();
    }
    return size;
  }
}
