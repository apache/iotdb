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
package org.apache.iotdb.db.engine.compaction.cross.utils;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Estimate the memory cost of one compaction task based on its corresponding implementation. */
public abstract class AbstractCompactionEstimator {

  protected Map<TsFileResource, TsFileSequenceReader> fileReaderCache = new HashMap<>();

  public long estimateCrossCompactionMemory(
      List<TsFileResource> seqResources, TsFileResource unseqResource) throws IOException {
    throw new RuntimeException(
        "This kind of estimator cannot be used to estimate cross space compaction task");
  }

  public long estimateInnerCompactionMemory(List<TsFileResource> resources) {
    throw new RuntimeException(
        "This kind of estimator cannot be used to estimate inner space compaction task");
  }

  /**
   * Construct the a new or get an existing TsFileSequenceReader of a TsFile.
   *
   * @return a TsFileSequenceReader
   */
  protected TsFileSequenceReader getFileReader(TsFileResource tsFileResource) throws IOException {
    TsFileSequenceReader reader = fileReaderCache.get(tsFileResource);
    if (reader == null) {
      reader = new TsFileSequenceReader(tsFileResource.getTsFilePath(), true, false);
      fileReaderCache.put(tsFileResource, reader);
    }
    return reader;
  }

  public void clear() throws IOException {
    for (TsFileSequenceReader sequenceReader : fileReaderCache.values()) {
      sequenceReader.close();
    }
    fileReaderCache.clear();
  }
}
