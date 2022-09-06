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
package org.apache.iotdb.backup.core.service;

import org.apache.iotdb.backup.core.exception.FileTransFormationException;
import org.apache.iotdb.backup.core.model.ValidationType;
import org.apache.iotdb.session.Session;

import java.io.File;

public interface FileValidationService {

  void dataValidateWithServer(String path, Session session, String charset, ValidationType type)
      throws Exception;

  default File validateFilePath(String filePath) throws FileTransFormationException {
    File fi = new File(filePath);
    if (fi.isFile()) {

    } else if (fi.isDirectory()) {
      throw new FileTransFormationException("given path is not a file,it is a directory");
    } else {
      throw new FileTransFormationException("file can not find");
    }
    return fi;
  }
}
