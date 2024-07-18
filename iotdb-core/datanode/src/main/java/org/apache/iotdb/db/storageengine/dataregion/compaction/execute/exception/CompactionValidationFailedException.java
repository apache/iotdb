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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.exception;

import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.util.List;

public class CompactionValidationFailedException extends RuntimeException {
  private List<TsFileResource> overlappedTsFileResources = null;

  public CompactionValidationFailedException(String msg) {
    super(msg);
  }

  public CompactionValidationFailedException(List<TsFileResource> overlappedTsFileResources) {
    super(
        "Failed to pass compaction validation, sequence files has overlap, file is "
            + overlappedTsFileResources);
    this.overlappedTsFileResources = overlappedTsFileResources;
  }

  public List<TsFileResource> getOverlappedTsFileResources() {
    return overlappedTsFileResources;
  }

  @Override
  public Throwable fillInStackTrace() {
    return this;
  }
}
