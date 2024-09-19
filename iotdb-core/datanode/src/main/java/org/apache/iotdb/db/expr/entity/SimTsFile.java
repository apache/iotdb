/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.expr.entity;

import org.apache.tsfile.read.common.TimeRange;

import java.util.Collections;
import java.util.List;
import java.util.function.Function;

public class SimTsFile {
  public long version;
  public TimeRange timeRange;

  private SimModFile modFile;
  public final Function<SimTsFile, SimModFile> modFileAllocator;
  public int startPosInMod;

  public SimTsFile(
      long version, TimeRange timeRange, Function<SimTsFile, SimModFile> modFileAllocator) {
    this.version = version;
    this.timeRange = timeRange;
    this.modFileAllocator = modFileAllocator;
  }

  public boolean shouldDelete(SimDeletion deletion) {
    return deletion.timeRange.overlaps(timeRange);
  }

  public SimModFile getModFileMayAllocate() {
    if (modFile == null) {
      modFile = modFileAllocator.apply(this);
      modFile.tsfileReferences.add(this);
      startPosInMod = modFile.mods.size();
    }
    return modFile;
  }

  public List<SimDeletion> getDeletions() {
    if (modFile == null) {
      return Collections.emptyList();
    }
    return modFile.mods.subList(startPosInMod, modFile.mods.size());
  }

  public boolean hasModFile() {
    return modFile != null;
  }
}
