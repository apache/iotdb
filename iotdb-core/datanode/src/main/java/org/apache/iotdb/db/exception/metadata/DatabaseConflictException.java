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

package org.apache.iotdb.db.exception.metadata;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.rpc.TSStatusCode;

public class DatabaseConflictException extends MetadataException {

  private final boolean isChild;

  private final String storageGroupPath;

  public DatabaseConflictException(final String path, final boolean isChild) {
    super(getMessage(path, isChild), TSStatusCode.DATABASE_CONFLICT.getStatusCode());
    this.isChild = isChild;
    storageGroupPath = path;
  }

  public boolean isChild() {
    return isChild;
  }

  public String getStorageGroupPath() {
    return storageGroupPath;
  }

  private static String getMessage(final String path, final boolean isChild) {
    if (isChild) {
      return String.format("some children of %s have already been created as database", path);
    } else {
      return String.format("%s has already been created as database", path);
    }
  }
}
