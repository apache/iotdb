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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.index;

import org.apache.iotdb.lsm.sstable.fileIO.ISSTableOutputStream;
import org.apache.iotdb.lsm.sstable.index.IDiskIndex;
import org.apache.iotdb.lsm.sstable.index.IDiskIndexWriter;
import org.apache.iotdb.lsm.sstable.index.IndexType;
import org.apache.iotdb.lsm.sstable.index.bplustree.writer.BPlusTreeWriter;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.util.HashMap;
import java.util.Map;

public class OffsetIndex implements IDiskIndex {
  private Map<String, Long> index;

  private IndexType indexType;

  public OffsetIndex(IndexType indexType) {
    this.index = new HashMap<>();
    this.indexType = indexType;
  }

  public void put(String key, Long value) {
    index.put(key, value);
  }

  @Override
  public long serialize(ISSTableOutputStream out) throws IOException {
    switch (indexType) {
      case BPlusTree:
        IDiskIndexWriter diskIndexWriter = new BPlusTreeWriter(out);
        return diskIndexWriter.write(index, false);
      default:
        throw new InvalidObjectException("not support indexType");
    }
  }
}
