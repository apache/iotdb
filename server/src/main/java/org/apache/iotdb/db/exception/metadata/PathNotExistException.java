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

import java.util.List;

public class PathNotExistException extends MetadataException {

  private static final String PATH_NOT_EXIST_WRONG_MESSAGE = "Path [%s] does not exist";

  public PathNotExistException(String path) {
    super(
        String.format(PATH_NOT_EXIST_WRONG_MESSAGE, path),
        TSStatusCode.TIMESERIES_NOT_EXIST.getStatusCode());
  }

  public PathNotExistException(String path, boolean isUserException) {
    super(
        String.format(PATH_NOT_EXIST_WRONG_MESSAGE, path),
        TSStatusCode.TIMESERIES_NOT_EXIST.getStatusCode(),
        isUserException);
  }

  public PathNotExistException(List<String> paths) {
    super(
        String.format(
            PATH_NOT_EXIST_WRONG_MESSAGE,
            paths.size() == 1
                ? paths.get(0)
                : paths.get(0) + " ... " + paths.get(paths.size() - 1)),
        TSStatusCode.TIMESERIES_NOT_EXIST.getStatusCode());
  }
}
