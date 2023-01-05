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

package org.apache.iotdb.confignode.persistence;

import org.apache.iotdb.commons.model.AbstractModelInformation;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;

import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.concurrent.ThreadSafe;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

@ThreadSafe
public class ModelInfo implements SnapshotProcessor {

  private static final Logger LOGGER = LoggerFactory.getLogger(ModelInfo.class);

  private static final String SNAPSHOT_FILENAME = "model_info.snapshot";

  private final Map<String, AbstractModelInformation> modelInfoMap;

  private final ReadWriteLock lock;

  public ModelInfo() {
    this.modelInfoMap = new HashMap<>();
    this.lock = new ReentrantReadWriteLock();
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    return false;
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {}
}
