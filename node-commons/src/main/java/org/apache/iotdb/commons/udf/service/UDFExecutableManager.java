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

package org.apache.iotdb.commons.udf.service;

import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.executable.ExecutableManager;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.service.IService;
import org.apache.iotdb.commons.service.ServiceType;
import org.apache.iotdb.commons.snapshot.SnapshotProcessor;

import java.io.File;
import java.io.IOException;

public class UDFExecutableManager extends ExecutableManager implements IService, SnapshotProcessor {

  private UDFExecutableManager(String temporaryLibRoot, String udfLibRoot) {
    super(temporaryLibRoot, udfLibRoot);
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // IService
  /////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public void start() throws StartupException {
    try {
      SystemFileFactory.INSTANCE.makeDirIfNecessary(temporaryLibRoot);
      SystemFileFactory.INSTANCE.makeDirIfNecessary(libRoot);
    } catch (Exception e) {
      throw new StartupException(e);
    }
  }

  @Override
  public void stop() {
    // nothing to do
  }

  @Override
  public ServiceType getID() {
    return ServiceType.UDF_EXECUTABLE_MANAGER_SERVICE;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // singleton instance holder
  /////////////////////////////////////////////////////////////////////////////////////////////////

  private static UDFExecutableManager INSTANCE = null;

  public static synchronized UDFExecutableManager setupAndGetInstance(
      String temporaryLibRoot, String udfLibRoot) {
    if (INSTANCE == null) {
      INSTANCE = new UDFExecutableManager(temporaryLibRoot, udfLibRoot);
    }
    return INSTANCE;
  }

  public static UDFExecutableManager getInstance() {
    return INSTANCE;
  }

  /////////////////////////////////////////////////////////////////////////////////////////////////
  // SnapshotProcessor
  /////////////////////////////////////////////////////////////////////////////////////////////////

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws IOException {
    return SnapshotUtils.takeSnapshotForDir(
            temporaryLibRoot,
            snapshotDir.getAbsolutePath() + File.separator + "ext" + File.separator + "temporary")
        && SnapshotUtils.takeSnapshotForDir(
            libRoot,
            snapshotDir.getAbsolutePath() + File.separator + "ext" + File.separator + "udf");
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws IOException {
    SnapshotUtils.loadSnapshotForDir(
        snapshotDir.getAbsolutePath() + File.separator + "ext" + File.separator + "temporary",
        temporaryLibRoot);
    SnapshotUtils.loadSnapshotForDir(
        snapshotDir.getAbsolutePath() + File.separator + "ext" + File.separator + "udf", libRoot);
  }
}
