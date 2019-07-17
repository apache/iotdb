/**
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

package org.apache.iotdb.db.engine.merge.manage;

import static org.apache.iotdb.db.engine.merge.task.MergeTask.MERGE_SUFFIX;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.engine.modification.Modification;
import org.apache.iotdb.db.engine.storagegroup.TsFileResource;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.query.reader.IPointReader;
import org.apache.iotdb.db.query.reader.resourceRelated.UnseqResourceMergeReader;
import org.apache.iotdb.tsfile.common.conf.TSFileConfig;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.read.TsFileSequenceReader;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerier;
import org.apache.iotdb.tsfile.read.controller.MetadataQuerierByFileImpl;
import org.apache.iotdb.tsfile.write.chunk.ChunkBuffer;
import org.apache.iotdb.tsfile.write.chunk.ChunkWriterImpl;
import org.apache.iotdb.tsfile.write.chunk.IChunkWriter;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;
import org.apache.iotdb.tsfile.write.writer.RestorableTsFileIOWriter;

/**
 * MergeResource manages files and caches of readers, writers, MeasurementSchemas and
 * modifications to avoid unnecessary object creations and file openings.
 */
public class MergeResource {

  private List<TsFileResource> seqFiles;
  private List<TsFileResource> unseqFiles;

  // future feature
  // keeping ChunkMetadata in memory avoids reading them again when we need to move unmerged
  // chunks to the merged file, but this may consume memory considerably
  private boolean keepChunkMetadata = false;

  private QueryContext mergeContext = new QueryContext();

  private Map<TsFileResource, TsFileSequenceReader> fileReaderCache = new HashMap<>();
  private Map<TsFileResource, RestorableTsFileIOWriter> fileWriterCache = new HashMap<>();
  private Map<TsFileResource, List<Modification>> modificationCache = new HashMap<>();
  private Map<TsFileResource, MetadataQuerier> metadataQuerierCache = new HashMap<>();
  private Map<String, MeasurementSchema> measurementSchemaMap = new HashMap<>();
  private Map<MeasurementSchema, IChunkWriter> chunkWriterCache = new HashMap<>();

  public MergeResource(
      List<TsFileResource> seqFiles,
      List<TsFileResource> unseqFiles) {
    this.seqFiles = seqFiles;
    this.unseqFiles = unseqFiles;
  }

  public void clear() throws IOException {
    for (TsFileSequenceReader sequenceReader : fileReaderCache.values()) {
      sequenceReader.close();
    }

    fileReaderCache.clear();
    fileWriterCache.clear();
    modificationCache.clear();
    metadataQuerierCache.clear();
    measurementSchemaMap.clear();
    chunkWriterCache.clear();
  }

  public  MeasurementSchema getSchema(String measurementId) {
    return measurementSchemaMap.get(measurementId);
  }

  public RestorableTsFileIOWriter getMergeFileWriter(TsFileResource resource) throws IOException {
    RestorableTsFileIOWriter writer = fileWriterCache.get(resource);
    if (writer == null) {
      writer = new RestorableTsFileIOWriter(new File(resource.getFile().getPath() + MERGE_SUFFIX));
      fileWriterCache.put(resource, writer);
    }
    return writer;
  }

  private MetadataQuerier getMetadataQuerier(TsFileResource seqFile) throws IOException {
    MetadataQuerier metadataQuerier = metadataQuerierCache.get(seqFile);
    if (metadataQuerier == null) {
      metadataQuerier = new MetadataQuerierByFileImpl(getFileReader(seqFile));
      metadataQuerierCache.put(seqFile, metadataQuerier);
    }
    return metadataQuerier;
  }

  public List<ChunkMetaData> queryChunkMetadata(Path path, TsFileResource seqFile)
      throws IOException {
    MetadataQuerier metadataQuerier = getMetadataQuerier(seqFile);
    List<ChunkMetaData> chunkMetaDataList = metadataQuerier.getChunkMetaDataList(path);
    if (!keepChunkMetadata) {
      metadataQuerier.clear();
    }
    return chunkMetaDataList;
  }

  public IPointReader getUnseqReader(Path path) throws IOException {
    return new UnseqResourceMergeReader(path, unseqFiles, mergeContext, null);
  }

  public IChunkWriter getChunkWriter(MeasurementSchema measurementSchema) {
    return chunkWriterCache.computeIfAbsent(measurementSchema,
        k -> new ChunkWriterImpl(k, new ChunkBuffer(k), TSFileConfig.pageCheckSizeThreshold));
  }

  public TsFileSequenceReader getFileReader(TsFileResource tsFileResource) throws IOException {
    TsFileSequenceReader reader = fileReaderCache.get(tsFileResource);
    if (reader == null) {
      reader = new TsFileSequenceReader(tsFileResource.getFile().getPath());
      fileReaderCache.put(tsFileResource, reader);
    }
    return reader;
  }

  public List<Modification> getModifications(TsFileResource tsFileResource, Path path) {
    // copy from TsFileResource so queries are not affected
    List<Modification> modifications = modificationCache.computeIfAbsent(tsFileResource,
        tsFileResource1 -> new ArrayList<>(tsFileResource.getModFile().getModifications()));
    List<Modification> pathModifications = new ArrayList<>();
    Iterator<Modification> modificationIterator = modifications.iterator();
    // each path is visited only once in a merge, so the modifications can be removed after visiting
    while (modificationIterator.hasNext()) {
      Modification modification = modificationIterator.next();
      if (modification.getPath().equals(path)) {
        pathModifications.add(modification);
        modificationIterator.remove();
      }
    }
    return pathModifications;
  }

  public void removeFileWriter(TsFileResource tsFileResource) throws IOException {
    RestorableTsFileIOWriter newFileWriter = fileWriterCache.remove(tsFileResource);
    if (newFileWriter != null) {
      newFileWriter.close();
      newFileWriter.getFile().delete();
    }
  }

  public void removeFileReader(TsFileResource resource) throws IOException {
    TsFileSequenceReader sequenceReader = fileReaderCache.remove(resource);
    if (sequenceReader != null) {
      sequenceReader.close();
    }
  }

  public List<TsFileResource> getSeqFiles() {
    return seqFiles;
  }

  public List<TsFileResource> getUnseqFiles() {
    return unseqFiles;
  }

  public void setSeqFiles(List<TsFileResource> seqFiles) {
    this.seqFiles = seqFiles;
  }

  public void setUnseqFiles(
      List<TsFileResource> unseqFiles) {
    this.unseqFiles = unseqFiles;
  }

  public Map<String, MeasurementSchema> getMeasurementSchemaMap() {
    return measurementSchemaMap;
  }
}
