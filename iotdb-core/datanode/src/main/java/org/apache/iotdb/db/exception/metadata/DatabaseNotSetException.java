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

public class DatabaseNotSetException extends MetadataException {

  private static final long serialVersionUID = 3739300272099030533L;

  public static final String DATABASE_NOT_SET = "Database is not set";

  public DatabaseNotSetException(String path) {
    super(
        String.format("%s for current seriesPath: [%s]", DATABASE_NOT_SET, path),
        TSStatusCode.DATABASE_NOT_EXIST.getStatusCode());
  }

  public DatabaseNotSetException(String path, boolean isUserException) {
    super(
        String.format("%s for current seriesPath: [%s]", DATABASE_NOT_SET, path),
        TSStatusCode.DATABASE_NOT_EXIST.getStatusCode(),
        isUserException);
  }

  public DatabaseNotSetException(String path, String reason) {
    super(
        String.format(
            "%s for current seriesPath: [%s], because %s", DATABASE_NOT_SET, path, reason));
  }
}
