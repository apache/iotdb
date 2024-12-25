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

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public class ConfigNodePipePluginMetaKeeper extends PipePluginMetaKeeper {

  protected final Map<String, String> jarNameToMd5Map;
  protected final Map<String, Integer> jarNameToReferenceCountMap;

  public ConfigNodePipePluginMetaKeeper() {
    super();

    jarNameToMd5Map = new HashMap<>();
    jarNameToReferenceCountMap = new HashMap<>();
  }

  public synchronized boolean containsJar(String jarName) {
    return jarNameToMd5Map.containsKey(jarName);
  }

  public synchronized boolean jarNameExistsAndMatchesMd5(String jarName, String md5) {
    return jarNameToMd5Map.containsKey(jarName) && jarNameToMd5Map.get(jarName).equals(md5);
  }

  public synchronized void addJarNameAndMd5(String jarName, String md5) {
    if (jarNameToReferenceCountMap.containsKey(jarName)) {
      jarNameToReferenceCountMap.put(jarName, jarNameToReferenceCountMap.get(jarName) + 1);
    } else {
      jarNameToReferenceCountMap.put(jarName, 1);
      jarNameToMd5Map.put(jarName, md5);
    }
  }

  public synchronized void removeJarNameAndMd5IfPossible(String jarName) {
    if (jarNameToReferenceCountMap.containsKey(jarName)) {
      int count = jarNameToReferenceCountMap.get(jarName);
      if (count == 1) {
        jarNameToReferenceCountMap.remove(jarName);
        jarNameToMd5Map.remove(jarName);
      } else {
        jarNameToReferenceCountMap.put(jarName, count - 1);
      }
    }
  }

  @Override
  public void processTakeSnapshot(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(jarNameToMd5Map.size(), outputStream);
    for (Map.Entry<String, String> entry : jarNameToMd5Map.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
      ReadWriteIOUtils.write(jarNameToReferenceCountMap.get(entry.getKey()), outputStream);
    }

    super.processTakeSnapshot(outputStream);
  }

  @Override
  public void processLoadSnapshot(InputStream inputStream) throws IOException {
    jarNameToMd5Map.clear();
    jarNameToReferenceCountMap.clear();

    final int jarSize = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < jarSize; i++) {
      final String jarName = ReadWriteIOUtils.readString(inputStream);
      final String md5 = ReadWriteIOUtils.readString(inputStream);
      final int count = ReadWriteIOUtils.readInt(inputStream);
      jarNameToMd5Map.put(jarName, md5);
      jarNameToReferenceCountMap.put(jarName, count);
    }

    super.processLoadSnapshot(inputStream);
  }
}
