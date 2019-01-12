package org.apache.iotdb.tsfile.read.controller;

import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;

import java.io.IOException;
import java.util.List;


public interface MetadataQuerier {

    List<ChunkMetaData> getChunkMetaDataList(Path path) throws IOException;

    TsFileMetaData getWholeFileMetadata();


    /**
     * this will load all chunk metadata of given paths into cache.
     *
     * call this method before calling getChunkMetaDataList() will
     * accelerate the reading of chunk metadata, which will only
     * read TsDeviceMetaData once
     */
    void loadChunkMetaDatas(List<Path> paths) throws IOException;

}
