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

import org.apache.iotdb.rpc.TSStatusCode;

public class StorageGroupAlreadySetException extends MetadataException {

  private static final long serialVersionUID = 9110669164701929779L;

  public StorageGroupAlreadySetException(String path) {
    super(getMessage(path, false), TSStatusCode.PATH_ALREADY_EXIST_ERROR.getStatusCode());
  }

  public StorageGroupAlreadySetException(String path, boolean hasChild) {
    super(getMessage(path, hasChild), TSStatusCode.PATH_ALREADY_EXIST_ERROR.getStatusCode());
  }

  private static String getMessage(String path, boolean hasChild) {
    if (hasChild) {
      return String.format("some children of %s have already been set to storage group", path);
    } else {
      return String.format("%s has already been set to storage group", path);
    }
  }
}
