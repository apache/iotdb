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
package org.apache.iotdb.commons.auth.user;

import org.apache.iotdb.commons.auth.AuthException;

import org.apache.thrift.TException;

import java.io.File;
import java.io.IOException;

public class LocalFileUserManager extends BasicUserManager {

  public LocalFileUserManager(String userDirPath) throws AuthException {
    super(new LocalFileUserAccessor(userDirPath));
  }

  @Override
  public boolean processTakeSnapshot(File snapshotDir) throws TException, IOException {
    return accessor.processTakeSnapshot(snapshotDir);
  }

  @Override
  public void processLoadSnapshot(File snapshotDir) throws TException, IOException {
    accessor.processLoadSnapshot(snapshotDir);
  }
}
