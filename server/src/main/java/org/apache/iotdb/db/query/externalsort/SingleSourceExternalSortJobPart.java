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
package org.apache.iotdb.db.query.externalsort;

import org.apache.iotdb.db.query.reader.chunk.ChunkReaderWrap;
import org.apache.iotdb.tsfile.read.reader.IPointReader;

import java.io.IOException;

public class SingleSourceExternalSortJobPart extends ExternalSortJobPart {

  private ChunkReaderWrap chunkReaderWrap;

  public SingleSourceExternalSortJobPart(ChunkReaderWrap chunkReaderWrap) {
    super(ExternalSortJobPartType.SINGLE_SOURCE);
    this.chunkReaderWrap = chunkReaderWrap;
  }

  @Override
  public IPointReader executeForIPointReader() throws IOException {
    return chunkReaderWrap.getIPointReader();
  }
}
