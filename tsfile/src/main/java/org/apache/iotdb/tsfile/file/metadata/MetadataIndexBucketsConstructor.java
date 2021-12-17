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

import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.common.conf.TSFileDescriptor;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.writer.TsFileOutput;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

public class MetadataIndexBucketsConstructor {

  private static final TSFileConfig config = TSFileDescriptor.getInstance().getConfig();

  private MetadataIndexBucketsConstructor() {
    throw new IllegalStateException("Utility class");
  }

  /**
   * Construct metadata index tree
   *
   * @param deviceTimeseriesMetadataMap device => TimeseriesMetadata list
   * @param tsFileOutput tsfile output
   */
  @SuppressWarnings("squid:S3776") // Suppress high Cognitive Complexity warning
  public static Pair<MetadataIndexBucket[], Integer> constructMetadataIndexBuckets(
      Map<String, List<TimeseriesMetadata>> deviceTimeseriesMetadataMap, TsFileOutput tsFileOutput)
      throws IOException {

    int timeseriesMetadataNum = 0;
    for (List<TimeseriesMetadata> timeseriesMetadataList : deviceTimeseriesMetadataMap.values()) {
      timeseriesMetadataNum += timeseriesMetadataList.size();
    }
    int bucketNum =
        (int) Math.ceil((double) timeseriesMetadataNum / config.getMaxDegreeOfIndexNode()); // B
    MetadataIndexBucket[] buckets = new MetadataIndexBucket[bucketNum];
    for (int i = 0; i < bucketNum; i++) {
      buckets[i] = new MetadataIndexBucket();
    }

    // for timeseriesMetadata of each device
    for (Entry<String, List<TimeseriesMetadata>> entry : deviceTimeseriesMetadataMap.entrySet()) {
      if (entry.getValue().isEmpty()) {
        continue;
      }
      for (int i = 0; i < entry.getValue().size(); i++) {
        TimeseriesMetadata timeseriesMetadata = entry.getValue().get(i);
        Path path = new Path(entry.getKey(), timeseriesMetadata.getMeasurementId());
        int bucketId = Math.abs(path.hashCode()) % bucketNum;
        long startOffset = tsFileOutput.getPosition();
        int size = timeseriesMetadata.serializeTo(tsFileOutput.wrapAsStream());
        buckets[bucketId].addEntry(
            new MetadataIndexBucketEntry(path.getFullPath(), startOffset, size));
      }
    }

    // for each bucket
    int bucketSerializeSize = 0;
    for (MetadataIndexBucket bucket : buckets) {
      bucket.orderEntries();
      bucketSerializeSize = Math.max(bucket.getSerializeSize(), bucketSerializeSize);
    }

    return new Pair<>(buckets, bucketSerializeSize);
  }
}
