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

package org.apache.iotdb.db.pipe.event.common.util;

import org.apache.iotdb.db.pipe.resource.PipeDataNodeResourceManager;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.io.File;
import java.util.Objects;

public final class PipeObjectPathUtil {

  private PipeObjectPathUtil() {}

  public static File resolveLinkedObjectFile(
      final TsFileResource tsFileResource, final String pipeName, final String relativePath) {
    Objects.requireNonNull(pipeName, "pipeName must not be null");

    if (tsFileResource == null || relativePath == null || relativePath.trim().isEmpty()) {
      return null;
    }

    return PipeDataNodeResourceManager.object()
        .getObjectFileHardlink(tsFileResource, relativePath, pipeName);
  }

  public static File resolveLinkedObjectDirectory(
      final TsFileResource tsFileResource, final String pipeName) {
    Objects.requireNonNull(pipeName, "pipeName must not be null");

    if (tsFileResource == null || tsFileResource.getTsFile() == null) {
      return null;
    }

    return PipeDataNodeResourceManager.object().getLinkedObjectDirectory(tsFileResource, pipeName);
  }
}
