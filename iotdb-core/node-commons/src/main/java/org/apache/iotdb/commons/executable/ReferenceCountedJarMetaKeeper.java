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

package org.apache.iotdb.commons.executable;

import org.apache.tsfile.utils.ReadWriteIOUtils;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

public class ReferenceCountedJarMetaKeeper {

  private final Map<String, String> jarNameToMd5Map = new HashMap<>();
  private final Map<String, Integer> jarNameToReferenceCountMap = new HashMap<>();

  public synchronized boolean containsJar(final String jarName) {
    return jarNameToMd5Map.containsKey(jarName);
  }

  public synchronized boolean needToSaveJar(final String jarName) {
    return !containsJar(jarName);
  }

  public synchronized boolean jarNameExistsAndMatchesMd5(final String jarName, final String md5) {
    return containsJar(jarName) && Objects.equals(jarNameToMd5Map.get(jarName), md5);
  }

  public synchronized void addReference(final String jarName, final String md5) {
    if (jarNameToReferenceCountMap.containsKey(jarName)) {
      jarNameToReferenceCountMap.put(jarName, jarNameToReferenceCountMap.get(jarName) + 1);
      return;
    }

    jarNameToReferenceCountMap.put(jarName, 1);
    jarNameToMd5Map.put(jarName, md5);
  }

  public synchronized void removeReference(final String jarName) {
    final Integer referenceCount = jarNameToReferenceCountMap.get(jarName);
    if (referenceCount == null || referenceCount <= 1) {
      jarNameToReferenceCountMap.remove(jarName);
      jarNameToMd5Map.remove(jarName);
      return;
    }

    jarNameToReferenceCountMap.put(jarName, referenceCount - 1);
  }

  public synchronized void clear() {
    jarNameToMd5Map.clear();
    jarNameToReferenceCountMap.clear();
  }

  public synchronized Map<String, String> getJarNameToMd5Map() {
    return new HashMap<>(jarNameToMd5Map);
  }

  public synchronized void serializeJarNameToMd5(final OutputStream outputStream)
      throws IOException {
    ReadWriteIOUtils.write(jarNameToMd5Map.size(), outputStream);
    for (final Map.Entry<String, String> entry : jarNameToMd5Map.entrySet()) {
      ReadWriteIOUtils.write(entry.getKey(), outputStream);
      ReadWriteIOUtils.write(entry.getValue(), outputStream);
    }
  }

  public synchronized void deserializeJarNameToMd5(final InputStream inputStream)
      throws IOException {
    clear();

    int size = ReadWriteIOUtils.readInt(inputStream);
    while (size > 0) {
      addReference(
          ReadWriteIOUtils.readString(inputStream), ReadWriteIOUtils.readString(inputStream));
      size--;
    }
  }

  public synchronized void serializeJarNameToMd5AndReferenceCount(final OutputStream outputStream)
      throws IOException {
    int size = 0;
    for (final Map.Entry<String, Integer> entry : jarNameToReferenceCountMap.entrySet()) {
      if (entry.getValue() > 0 && jarNameToMd5Map.containsKey(entry.getKey())) {
        size++;
      }
    }

    ReadWriteIOUtils.write(size, outputStream);
    for (final Map.Entry<String, Integer> entry : jarNameToReferenceCountMap.entrySet()) {
      final String jarName = entry.getKey();
      final int referenceCount = entry.getValue();
      if (referenceCount <= 0 || !jarNameToMd5Map.containsKey(jarName)) {
        continue;
      }
      ReadWriteIOUtils.write(jarName, outputStream);
      ReadWriteIOUtils.write(jarNameToMd5Map.get(jarName), outputStream);
      ReadWriteIOUtils.write(referenceCount, outputStream);
    }
  }

  public synchronized void deserializeJarNameToMd5AndReferenceCount(final InputStream inputStream)
      throws IOException {
    clear();

    final int jarSize = ReadWriteIOUtils.readInt(inputStream);
    for (int i = 0; i < jarSize; i++) {
      final String jarName = ReadWriteIOUtils.readString(inputStream);
      final String md5 = ReadWriteIOUtils.readString(inputStream);
      final int referenceCount = ReadWriteIOUtils.readInt(inputStream);
      if (referenceCount > 0) {
        jarNameToMd5Map.put(jarName, md5);
        jarNameToReferenceCountMap.put(jarName, referenceCount);
      }
    }
  }
}
