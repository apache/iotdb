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
package org.apache.iotdb.db.engine.cache;

import java.io.IOException;
import java.util.List;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.control.FileReaderManager;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetadata;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * This class is used to read metadata(<code>TsFileMetadata</code> and ChunkMetadata of a path
 */
class TsFileMetadataUtils {

  /**
   * get TsFileMetadata of a closed TsFile.
   *
   * @param resource -given TsFile
   * @return -meta data
   */
  static TsFileMetadata getTsFileMetadata(TsFileResource resource) throws IOException {
    if (!resource.isClosed()) {
      throw new IOException("The TsFile is not closed: " + resource.getFile().getAbsolutePath());
    }
    TsFileSequenceReader reader = FileReaderManager.getInstance().get(resource, true);
    return reader.readFileMetadata();
  }

  /**
   * get ChunkMetadata List of a path in the tsfile resource
   */
  static List<ChunkMetadata> getChunkMetadataList(Path path, TsFileResource resource) throws IOException {
    if (!resource.isClosed()) {
      throw new IOException("The TsFile is not closed: " + resource.getFile().getAbsolutePath());
    }
    TsFileSequenceReader tsFileReader = FileReaderManager.getInstance().get(resource, true);
    return tsFileReader.getChunkMetadataList(path);
  }
}
