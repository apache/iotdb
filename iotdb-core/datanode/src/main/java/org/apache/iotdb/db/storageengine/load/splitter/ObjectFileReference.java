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

package org.apache.iotdb.db.storageengine.load.splitter;

import java.io.File;
import java.util.Objects;

public class ObjectFileReference {

  private final File parentDir;
  private final String sourceRelativePath;
  private final String targetRelativePath;

  public ObjectFileReference(
      final File parentDir, final String sourceRelativePath, final String targetRelativePath) {
    this.parentDir = Objects.requireNonNull(parentDir, "parentDir");
    this.sourceRelativePath = Objects.requireNonNull(sourceRelativePath, "sourceRelativePath");
    this.targetRelativePath = Objects.requireNonNull(targetRelativePath, "targetRelativePath");
  }

  public File getParentDir() {
    return parentDir;
  }

  public String getSourceRelativePath() {
    return sourceRelativePath;
  }

  public String getTargetRelativePath() {
    return targetRelativePath;
  }

  @Override
  public boolean equals(final Object o) {
    if (this == o) {
      return true;
    }
    if (!(o instanceof ObjectFileReference)) {
      return false;
    }
    final ObjectFileReference that = (ObjectFileReference) o;
    return parentDir.equals(that.parentDir)
        && sourceRelativePath.equals(that.sourceRelativePath)
        && targetRelativePath.equals(that.targetRelativePath);
  }

  @Override
  public int hashCode() {
    return Objects.hash(parentDir, sourceRelativePath, targetRelativePath);
  }
}
