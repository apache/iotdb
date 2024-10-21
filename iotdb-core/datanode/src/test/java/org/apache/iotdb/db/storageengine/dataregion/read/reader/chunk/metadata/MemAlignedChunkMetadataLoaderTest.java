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

package org.apache.iotdb.db.storageengine.dataregion.read.reader.chunk.metadata;

import org.apache.iotdb.commons.path.AlignedFullPath;
import org.apache.iotdb.db.queryengine.execution.fragment.FragmentInstanceContext;
import org.apache.iotdb.db.storageengine.dataregion.memtable.ReadOnlyMemChunk;
import org.apache.iotdb.db.storageengine.dataregion.tsfile.TsFileResource;

import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.AlignedTimeSeriesMetadata;
import org.apache.tsfile.file.metadata.IChunkMetadata;
import org.apache.tsfile.file.metadata.ITimeSeriesMetadata;
import org.junit.Test;
import org.mockito.Mockito;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class MemAlignedChunkMetadataLoaderTest {

  @Test
  public void testLoadChunkMetadataList() {
    AlignedFullPath path = Mockito.mock(AlignedFullPath.class);
    TsFileResource resource = Mockito.mock(TsFileResource.class);
    FragmentInstanceContext context = Mockito.mock(FragmentInstanceContext.class);
    List<IChunkMetadata> chunkMetadataList1 = new ArrayList<>();
    IChunkMetadata chunkMetadata1 = Mockito.mock(AlignedChunkMetadata.class);
    chunkMetadataList1.add(chunkMetadata1);

    Mockito.when(resource.getChunkMetadataList(path)).thenReturn(chunkMetadataList1);

    Mockito.when(chunkMetadata1.needSetChunkLoader()).thenReturn(true);
    Mockito.when(resource.getTsFilePath()).thenReturn("1-1-0-0.tsfile");
    Mockito.when(resource.isClosed()).thenReturn(false);
    Mockito.when(context.isDebug()).thenReturn(false);

    List<ReadOnlyMemChunk> memChunks = new ArrayList<>();
    ReadOnlyMemChunk readOnlyMemChunk = Mockito.mock(ReadOnlyMemChunk.class);
    memChunks.add(readOnlyMemChunk);
    Mockito.when(readOnlyMemChunk.isEmpty()).thenReturn(false);
    Mockito.when(resource.getReadOnlyMemChunk(path)).thenReturn(memChunks);

    IChunkMetadata chunkMetadata2 = Mockito.mock(AlignedChunkMetadata.class);
    Mockito.when(readOnlyMemChunk.getChunkMetaData()).thenReturn(chunkMetadata2);
    Mockito.when(resource.getVersion()).thenReturn(1L);

    MemAlignedChunkMetadataLoader loader =
        new MemAlignedChunkMetadataLoader(resource, path, context, null, true);
    ITimeSeriesMetadata timeSeriesMetadata = Mockito.mock(AlignedTimeSeriesMetadata.class);

    List<IChunkMetadata> chunkMetadataList = loader.loadChunkMetadataList(timeSeriesMetadata);

    assertEquals(2, chunkMetadataList.size());
    assertEquals(chunkMetadata1, chunkMetadataList.get(0));
    assertEquals(chunkMetadata2, chunkMetadataList.get(1));
  }
}
