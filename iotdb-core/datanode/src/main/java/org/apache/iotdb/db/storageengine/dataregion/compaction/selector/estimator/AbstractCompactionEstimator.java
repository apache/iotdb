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

package org.apache.iotdb.db.storageengine.dataregion.compaction.selector.estimator;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.storageengine.dataregion.flush.CompressionRatio;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Estimate the memory cost of one compaction task with specific source files based on its
 * corresponding implementation.
 */
public abstract class AbstractCompactionEstimator {

  protected Map<TsFileResource, TsFileSequenceReader> fileReaderCache = new HashMap<>();

  protected IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  protected long compressionRatio = (long) CompressionRatio.getInstance().getRatio() + 1;

  /**
   * Estimate the memory cost of compacting the unseq file and its corresponding overlapped seq
   * files in cross space compaction task.
   *
   * @throws IOException if io errors occurred
   */
  public abstract long estimateCrossCompactionMemory(
      List<TsFileResource> seqResources, TsFileResource unseqResource) throws IOException;

  /** Estimate the memory cost of compacting the source files in inner space compaction task. */
  public abstract long estimateInnerCompactionMemory(List<TsFileResource> resources)
      throws IOException;

  /**
   * Construct a new or get an existing TsFileSequenceReader of a TsFile.
   *
   * @throws IOException if io errors occurred
   */
  protected TsFileSequenceReader getFileReader(TsFileResource tsFileResource) throws IOException {
    TsFileSequenceReader reader = fileReaderCache.get(tsFileResource);
    if (reader == null) {
      reader = new TsFileSequenceReader(tsFileResource.getTsFilePath(), true, false);
      fileReaderCache.put(tsFileResource, reader);
    }
    return reader;
  }

  public void close() throws IOException {
    for (TsFileSequenceReader reader : fileReaderCache.values()) {
      reader.close();
    }
    fileReaderCache.clear();
  }
}
