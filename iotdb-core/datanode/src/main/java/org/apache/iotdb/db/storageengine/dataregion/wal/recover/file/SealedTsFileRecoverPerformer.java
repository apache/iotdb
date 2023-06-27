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

package org.apache.iotdb.db.storageengine.dataregion.wal.recover.file;

import org.apache.iotdb.db.exception.DataRegionException;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import java.io.IOException;

/** This class is used to help recover all sealed TsFiles, except unsealed TsFile at zero level. */
public class SealedTsFileRecoverPerformer extends AbstractTsFileRecoverPerformer {
  public SealedTsFileRecoverPerformer(TsFileResource tsFileResource) {
    super(tsFileResource);
  }

  /**
   * Recover sealed TsFile, including load .resource file (reconstruct when necessary) and truncate
   * the file to remaining corrected data.
   *
   * @throws DataRegionException when failing to recover.
   * @throws IOException when failing to end file.
   */
  public void recover() throws DataRegionException, IOException {
    super.recoverWithWriter();

    if (hasCrashed()) {
      writer.endFile();
      try {
        reconstructResourceFile();
      } catch (IOException e) {
        throw new DataRegionException(
            "Failed recover the resource file: "
                + tsFileResource.getTsFilePath()
                + TsFileResource.RESOURCE_SUFFIX
                + e);
      }
    }
  }
}
