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

package org.apache.iotdb.tsfile.read.common;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetadata;
import org.apache.iotdb.tsfile.read.reader.page.PageReader;

public class ChunkSuit4Tri {

  public ChunkMetadata chunkMetadata; // fixed info, including version, dataType, stepRegress

  public int lastReadPos =
      0; // dynamic maintained globally, starting from 0, incremental, never decrease

  // TODO ATTENTION: YOU HAVE TO ENSURE THAT THERE IS ONLY ONE PAGE IN A CHUNK,
  //  BECAUSE THE WHOLE IMPLEMENTATION IS BASED ON THIS ASSUMPTION.
  //  OTHERWISE, PAGEREADER IS FOR THE FIRST PAGE IN THE CHUNK WHILE
  //  STEPREGRESS IS FOR THE LAST PAGE IN THE CHUNK (THE MERGE OF STEPREGRESS IS ASSIGN DIRECTLY),
  //  WHICH WILL INTRODUCE BUGS!
  public PageReader pageReader; // bears fixed plain timeBuffer and valueBuffer
  // pageReader does not refer to the same deleteInterval as those in chunkMetadata
  // after chunkMetadata executes insertIntoSortedDeletions

  public ChunkSuit4Tri(ChunkMetadata chunkMetadata) {
    this.chunkMetadata = chunkMetadata;
    this.lastReadPos = 0;
  }

  public ChunkSuit4Tri(ChunkMetadata chunkMetadata, PageReader pageReader) {
    this.chunkMetadata = chunkMetadata;
    this.pageReader = pageReader;
    this.lastReadPos = 0;
  }
}
