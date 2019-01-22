/**
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
package org.apache.iotdb.db.writelog.recover;

import org.apache.iotdb.db.engine.filenode.FileNodeManager;
import org.apache.iotdb.db.exception.FileNodeManagerException;
import org.apache.iotdb.db.exception.FileNodeProcessorException;
import org.apache.iotdb.db.exception.RecoverException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class FileNodeRecoverPerformer implements RecoverPerformer {

  private static final Logger logger = LoggerFactory.getLogger(FileNodeRecoverPerformer.class);

  /**
   * If the storage group is "root.a.b", then the identifier of a bufferwrite processor will be
   * "root.a.b-bufferwrite", and the identifier of an overflow processor will be
   * "root.a.b-overflow".
   */
  private String identifier;

  public FileNodeRecoverPerformer(String identifier) {
    this.identifier = identifier;
  }

  @Override
  public void recover() throws RecoverException {
    try {
      FileNodeManager.getInstance().recoverFileNode(getFileNodeName());
    } catch (FileNodeProcessorException | FileNodeManagerException e) {
      logger.error("Cannot recover filenode {}", identifier);
      throw new RecoverException(e);
    }
  }

  public String getFileNodeName() {
    return identifier.split("-")[0];
  }
}
