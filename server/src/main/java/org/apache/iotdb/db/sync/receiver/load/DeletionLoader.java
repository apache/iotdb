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
package org.apache.iotdb.db.sync.receiver.load;

import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.exception.sync.PipeDataLoadException;
import org.apache.iotdb.db.exception.sync.PipeDataLoadUnbearableException;

/** This loader is used to load deletion plan. */
public class DeletionLoader implements ILoader {

  private Deletion deletion;

  public DeletionLoader(Deletion deletion) {
    this.deletion = deletion;
  }

  @Override
  public void load() throws PipeDataLoadException {
    if (IoTDBDescriptor.getInstance().getConfig().isReadOnly()) {
      throw new PipeDataLoadUnbearableException("storage engine readonly");
    }
    try {
      StorageEngine.getInstance()
          .delete(deletion.getPath(), deletion.getStartTime(), deletion.getEndTime(), 0, null);
    } catch (Exception e) {
      throw new PipeDataLoadUnbearableException(e.getMessage());
    }
  }
}
