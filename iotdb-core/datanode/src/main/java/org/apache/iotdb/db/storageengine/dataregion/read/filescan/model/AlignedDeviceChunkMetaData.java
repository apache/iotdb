package org.apache.iotdb.db.storageengine.dataregion.read.filescan.model;

import org.apache.tsfile.file.metadata.AlignedChunkMetadata;
import org.apache.tsfile.file.metadata.IDeviceID;

import java.util.List;

public class AlignedDeviceChunkMetaData extends AbstractDeviceChunkMetaData {

  List<AlignedChunkMetadata> alignedChunkMetadataList;

  public AlignedDeviceChunkMetaData(
      IDeviceID devicePath, List<AlignedChunkMetadata> alignedChunkMetadataList) {
    super(devicePath);
    this.alignedChunkMetadataList = alignedChunkMetadataList;
  }

  public List<AlignedChunkMetadata> getAlignedChunkMetadataList() {
    return alignedChunkMetadataList;
  }

  @Override
  public boolean isAligned() {
    return true;
  }
}
