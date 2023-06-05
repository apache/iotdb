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
package org.apache.iotdb.db.query.reader;

import org.apache.iotdb.db.engine.cache.BloomFilterCache;
import org.apache.iotdb.db.engine.cache.TimeSeriesMetadataCache;
import org.apache.iotdb.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.iotdb.tsfile.utils.BloomFilter;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.File;
import java.io.IOException;

public class UpdateCacheTsFileIOWriter extends TsFileIOWriter {

  public UpdateCacheTsFileIOWriter(File file, boolean enableMemoryControl, long maxMetadataSize)
      throws IOException {
    super(file, enableMemoryControl, maxMetadataSize);
  }

  private static final BloomFilterCache BLOOM_FILTER_CACHE = BloomFilterCache.getInstance();

  private static final TimeSeriesMetadataCache TIME_SERIES_METADATA_CACHE =
      TimeSeriesMetadataCache.getInstance();

  @Override
  protected void updateCache(String device, TimeseriesMetadata timeseriesMetadata) {
    TIME_SERIES_METADATA_CACHE.updateCache(file.getPath(), device, timeseriesMetadata);
  }

  @Override
  protected void updateCache(BloomFilter bloomFilter) {
    BLOOM_FILTER_CACHE.updateCache(file.getPath(), bloomFilter);
  }
}
