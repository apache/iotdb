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

package org.apache.iotdb.commons.pipe.agent.plugin.meta;

import org.apache.iotdb.commons.executable.ReferenceCountedJarMetaKeeper;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

public class ConfigNodePipePluginMetaKeeper extends PipePluginMetaKeeper {

  protected final ReferenceCountedJarMetaKeeper jarMetaKeeper;

  public ConfigNodePipePluginMetaKeeper() {
    super();

    jarMetaKeeper = new ReferenceCountedJarMetaKeeper();
  }

  public synchronized boolean containsJar(String jarName) {
    return jarMetaKeeper.containsJar(jarName);
  }

  public synchronized boolean jarNameExistsAndMatchesMd5(String jarName, String md5) {
    return jarMetaKeeper.jarNameExistsAndMatchesMd5(jarName, md5);
  }

  public synchronized void addJarNameAndMd5(String jarName, String md5) {
    jarMetaKeeper.addReference(jarName, md5);
  }

  public synchronized void removeJarNameAndMd5IfPossible(String jarName) {
    jarMetaKeeper.removeReference(jarName);
  }

  @Override
  public void processTakeSnapshot(OutputStream outputStream) throws IOException {
    jarMetaKeeper.serializeJarNameToMd5AndReferenceCount(outputStream);

    super.processTakeSnapshot(outputStream);
  }

  @Override
  public void processLoadSnapshot(InputStream inputStream) throws IOException {
    jarMetaKeeper.deserializeJarNameToMd5AndReferenceCount(inputStream);

    super.processLoadSnapshot(inputStream);
  }
}
