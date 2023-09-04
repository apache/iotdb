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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.reader;

import org.apache.iotdb.db.storageengine.dataregion.compaction.io.CompactionTsFileReader;
import org.apache.iotdb.tsfile.file.header.PageHeader;

import java.io.IOException;
import java.nio.ByteBuffer;

public class LazyPageLoader {
  private CompactionTsFileReader reader;
  private PageHeader pageHeader;
  private long startOffsetInFile;

  public LazyPageLoader(
      CompactionTsFileReader reader, long startOffsetInFile, PageHeader pageHeader) {
    this.reader = reader;
    this.startOffsetInFile = startOffsetInFile;
    this.pageHeader = pageHeader;
  }

  public LazyPageLoader() {}

  public ByteBuffer loadPage() throws IOException {
    return reader.readPageWithoutUncompressing(startOffsetInFile, pageHeader.getCompressedSize());
  }

  public PageHeader getPageHeader() {
    return pageHeader;
  }

  public boolean isEmpty() {
    return reader == null;
  }
}
