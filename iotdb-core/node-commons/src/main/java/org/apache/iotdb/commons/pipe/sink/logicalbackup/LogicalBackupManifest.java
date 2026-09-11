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

package org.apache.iotdb.commons.pipe.sink.logicalbackup;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class LogicalBackupManifest {

  public String formatName = LogicalBackupFormat.FORMAT_NAME;
  public String formatVersion = LogicalBackupFormat.FORMAT_VERSION;
  public String backupId;
  public String status;
  public String pipeName;
  public long pipeCreationTime;
  public String sourceClusterId;
  public String sourceVersion;
  public String timestampPrecision;
  public String createdAt;
  public String closedAt;
  public String streamId;
  public int regionId;
  public String sinkTaskId;
  public String streamType;
  public String fsyncPolicy;
  public long segmentSizeBytes;
  public int maxRecordBytes;
  public long firstSequence = -1;
  public long lastSequence = -1;
  public long lastDurableSequence = -1;
  public String lastEventGroupId;
  public String lastEventDigest;
  public long lastEventFirstSequence = -1;
  public long skippedEventCount;
  public boolean recovered;
  public Map<String, Long> operationCounts = new LinkedHashMap<>();
  public List<Segment> segments = new ArrayList<>();

  public static class Segment {
    public String file;
    public long segmentId;
    public long firstSequence = -1;
    public long lastSequence = -1;
    public long recordCount;
    public long sizeBytes;
    public String sha256;
    public String status;
  }
}
