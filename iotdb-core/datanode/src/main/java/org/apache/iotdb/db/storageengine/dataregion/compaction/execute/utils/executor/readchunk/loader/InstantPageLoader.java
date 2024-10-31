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

package org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.readchunk.loader;

import org.apache.iotdb.db.storageengine.dataregion.compaction.execute.utils.executor.ModifiedStatus;

import org.apache.tsfile.compress.IUnCompressor;
import org.apache.tsfile.encrypt.EncryptParameter;
import org.apache.tsfile.encrypt.IDecryptor;
import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.exception.write.PageException;
import org.apache.tsfile.file.header.PageHeader;
import org.apache.tsfile.file.metadata.ChunkMetadata;
import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.EncryptionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;
import org.apache.tsfile.write.chunk.AlignedChunkWriterImpl;

import java.io.IOException;
import java.nio.ByteBuffer;

import static org.apache.tsfile.read.reader.chunk.ChunkReader.decryptAndUncompressPageData;
import static org.apache.tsfile.read.reader.chunk.ChunkReader.uncompressPageData;

public class InstantPageLoader extends PageLoader {

  private ByteBuffer pageData;

  private EncryptParameter encryptParam;

  public InstantPageLoader() {}

  public InstantPageLoader(
      String file,
      PageHeader pageHeader,
      ByteBuffer pageData,
      CompressionType compressionType,
      TSDataType dataType,
      TSEncoding encoding,
      ChunkMetadata chunkMetadata,
      ModifiedStatus modifiedStatus,
      EncryptParameter encryptParam) {
    super(file, pageHeader, compressionType, dataType, encoding, chunkMetadata, modifiedStatus);
    this.pageData = pageData;
    this.encryptParam = encryptParam;
  }

  @Override
  public ByteBuffer getCompressedData() {
    return pageData;
  }

  @Override
  public ByteBuffer getUnCompressedData() throws IOException {
    IUnCompressor unCompressor = IUnCompressor.getUnCompressor(compressionType);
    IDecryptor decryptor = IDecryptor.getDecryptor(encryptParam);
    if (decryptor == null || decryptor.getEncryptionType() == EncryptionType.UNENCRYPTED) {
      return uncompressPageData(pageHeader, unCompressor, pageData);
    } else {
      return decryptAndUncompressPageData(pageHeader, unCompressor, pageData, decryptor);
    }
  }

  @Override
  public void flushToTimeChunkWriter(AlignedChunkWriterImpl alignedChunkWriter)
      throws PageException {
    alignedChunkWriter.writePageHeaderAndDataIntoTimeBuff(pageData, pageHeader);
    clear();
  }

  @Override
  public void flushToValueChunkWriter(
      AlignedChunkWriterImpl alignedChunkWriter, int valueColumnIndex)
      throws IOException, PageException {
    if (isEmpty()) {
      alignedChunkWriter.getValueChunkWriterByIndex(valueColumnIndex).writeEmptyPageToPageBuffer();
    } else {
      alignedChunkWriter
          .getValueChunkWriterByIndex(valueColumnIndex)
          .writePageHeaderAndDataIntoBuff(pageData, pageHeader);
      clear();
    }
  }

  @Override
  public boolean isEmpty() {
    return pageHeader == null
        || pageData == null
        || pageHeader.getUncompressedSize() == 0
        || this.modifiedStatus == ModifiedStatus.ALL_DELETED;
  }

  @Override
  public void clear() {
    this.deleteIntervalList = null;
    this.pageHeader = null;
    this.pageData = null;
  }
}
