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
package org.apache.iotdb.db.engine.compaction.performer;

import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;

import java.io.IOException;
import java.util.List;

/**
 * CompactionPerformer is used to compact multiple files into one or multiple files. Different
 * performers may use different implementation to achieve this goal. Some may read chunk directly
 * from tsfile, and some may using query tools to read data point by point from tsfile. Notice, not
 * all kinds of Performer can be used for all kinds of compaction tasks!
 */
public abstract class AbstractCompactionPerformer {
  protected List<TsFileResource> seqFiles;
  protected List<TsFileResource> unseqFiles;

  public abstract void perform()
      throws IOException, MetadataException, StorageEngineException, InterruptedException;
}
