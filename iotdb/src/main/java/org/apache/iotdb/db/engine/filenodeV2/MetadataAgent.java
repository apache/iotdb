package org.apache.iotdb.db.engine.filenodeV2;

import java.util.List;
import org.apache.iotdb.tsfile.file.metadata.ChunkGroupMetaData;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.TsFileMetaData;
import org.apache.iotdb.tsfile.read.common.Path;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.MeasurementSchema;

public class MetadataAgent {


  public MeasurementSchema getMeasurementSchema(TsFileResourceV2 tsFileResource, String measurement) {
   return null;
  }

  public Pair<Long, Long> getTimeInterval(TsFileResourceV2 tsFileResource, String device) {
    return null;
  }

//  public List<ChunkGroupMetaData> getChunkGroupMetadataList(TsFileResourceV2 tsFileResource, String device) {
//
//  }

  public List<ChunkMetaData> getChunkMetadataList(TsFileResourceV2 tsFileResource, Path series) {
    return null;
  }




}
