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

package org.apache.iotdb.commons.pipe.plugin.meta;

import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;

public class ConfigNodePipePluginMetaKeeper extends PipePluginMetaKeeper {

  protected final Map<String, String> jarNameToMd5Map;
  protected final Map<String, Integer> jarNameToReferrenceCountMap;

  public ConfigNodePipePluginMetaKeeper() {
    super();
    jarNameToMd5Map = new HashMap<>();
    jarNameToReferrenceCountMap = new HashMap<>();
  }

  public synchronized boolean containsJar(String jarName) {
    return jarNameToMd5Map.containsKey(jarName);
  }

  public synchronized boolean jarNameExistsAndMatchesMd5(String jarName, String md5) {
    return jarNameToMd5Map.containsKey(jarName) && jarNameToMd5Map.get(jarName).equals(md5);
  }

  public synchronized void addJarNameAndMd5(String jarName, String md5) {
    if (jarNameToReferrenceCountMap.containsKey(jarName)) {
      jarNameToReferrenceCountMap.put(jarName, jarNameToReferrenceCountMap.get(jarName) + 1);
    } else {
      jarNameToReferrenceCountMap.put(jarName, 1);
      jarNameToMd5Map.put(jarName, md5);
    }
  }

  public synchronized void removeJarNameAndMd5IfPossible(String jarName) {
    if (jarNameToReferrenceCountMap.containsKey(jarName)) {
      int count = jarNameToReferrenceCountMap.get(jarName);
      if (count == 1) {
        jarNameToReferrenceCountMap.remove(jarName);
        jarNameToMd5Map.remove(jarName);
      } else {
        jarNameToReferrenceCountMap.put(jarName, count - 1);
      }
    }
  }

  public void processTakeSnapshot(OutputStream outputStream) throws IOException {
    ReadWriteIOUtils.write(jarNameToMd5Map.size(), outputStream);
    for (Map.Entry<String, String> entry : jarNameToMd5Map.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
      ReadWriteIOUtils.write(jarNameToReferrenceCountMap.get(entry.getKey()), outputStream);
    }

    ReadWriteIOUtils.write(pipeNameToPipeMetaMap.size(), outputStream);
    for (PipePluginMeta pipePluginMeta : pipeNameToPipeMetaMap.values()) {
      ReadWriteIOUtils.write(pipePluginMeta.serialize(), outputStream);
    }
  }

  public void processLoadSnapshot(InputStream inputStream) throws IOException {
    clear();

    final int jarSize = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < jarSize; i++) {
      final String jarName = ReadWriteIOUtils.readString(inputStream);
      final String md5 = ReadWriteIOUtils.readString(inputStream);
      final int count = ReadWriteIOUtils.readInt(inputStream);
      jarNameToMd5Map.put(jarName, md5);
      jarNameToReferrenceCountMap.put(jarName, count);
    }

    final int pipePluginMetaSize = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < pipePluginMetaSize; i++) {
      final PipePluginMeta pipePluginMeta = PipePluginMeta.deserialize(inputStream);
      addPipePluginMeta(pipePluginMeta.getPluginName().toUpperCase(), pipePluginMeta);
    }
  }

  private void clear() {
    pipeNameToPipeMetaMap.clear();
    jarNameToMd5Map.clear();
    jarNameToReferrenceCountMap.clear();
  }
}
