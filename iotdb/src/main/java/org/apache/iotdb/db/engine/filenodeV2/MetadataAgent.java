/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.iotdb.db.engine.filenodeV2;

import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class MetadataAgent {


  public MeasurementSchema getMeasurementSchema(TsFileResourceV2 tsFileResource, String measurement) {
   return null;
  }

  public Pair<Long, Long> getTimeInterval(TsFileResourceV2 tsFileResource, String device) {
    return null;
  }

//  public List<ChunkGroupMetaData> getChunkGroupMetadataList(TsFileResourceV2 tsFileResource, String device) {
//
//  }

  public List<ChunkMetaData> getChunkMetadataList(TsFileResourceV2 tsFileResource, Path series) {
    return null;
  }




}
