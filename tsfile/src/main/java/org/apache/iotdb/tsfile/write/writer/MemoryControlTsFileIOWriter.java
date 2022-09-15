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

package org.apache.iotdb.tsfile.write.writer;

import org.apache.iotdb.tsfile.read.common.Path;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;

/**
 * This writer control the total size of chunk metadata to avoid OOM when writing massive
 * timeseries. <b>This writer can only be used in the scenarios where the chunk is written in
 * order.</b> The order means lexicographical order and time order. The lexicographical order
 * requires that, if the writer is going to write a series <i>S</i>, all data of the all series
 * smaller than <i>S</i> in lexicographical order has been written to the writer. The time order
 * requires that, for a single series <i>S</i>, if the writer is going to write a chunk <i>C</i> of
 * it, all chunks of <i>S</i> whose start time is smaller than <i>C</i> should have been written to
 * the writer. If you do not comply with the above requirements, metadata index tree may be
 * generated incorrectly. As a result, the file cannot be queried correctly.
 */
public class MemoryControlTsFileIOWriter extends TsFileIOWriter {
  private static final Logger LOG = LoggerFactory.getLogger(MemoryControlTsFileIOWriter.class);
  protected long maxMetadataSize;
  protected long currentChunkMetadataSize = 0L;
  protected File chunkMetadataTempFile;
  protected LocalTsFileOutput tempOutput;
  protected volatile boolean hasChunkMetadataInDisk = false;
  protected String currentSeries = null;
  // record the total num of path in order to make bloom filter
  protected int pathCount = 0;
  Path lastSerializePath = null;

  public static final String CHUNK_METADATA_TEMP_FILE_SUFFIX = ".cmt";

  public MemoryControlTsFileIOWriter(File file, long maxMetadataSize) throws IOException {
    super(file);
    this.maxMetadataSize = maxMetadataSize;
    this.chunkMetadataTempFile = new File(file.getAbsoluteFile() + CHUNK_METADATA_TEMP_FILE_SUFFIX);
  }
}
