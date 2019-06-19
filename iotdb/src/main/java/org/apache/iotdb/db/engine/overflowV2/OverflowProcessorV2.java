package org.apache.iotdb.db.engine.overflowV2;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.iotdb.db.engine.AbstractUnsealedDataFileProcessorV2;
import org.apache.iotdb.db.engine.memtable.Callback;
import org.apache.iotdb.db.engine.querycontext.ReadOnlyMemChunk;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.tsfile.file.metadata.ChunkMetaData;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.FileSchema;

public class OverflowProcessorV2 extends AbstractUnsealedDataFileProcessorV2 {

  public OverflowProcessorV2(String storageGroupName, File file,
      FileSchema fileSchema,
      VersionController versionController,
      Callback closeBufferWriteProcessor) throws IOException {
    super(storageGroupName, file, fileSchema, versionController, closeBufferWriteProcessor);
  }

  @Override
  public Pair<ReadOnlyMemChunk, List<ChunkMetaData>> queryUnsealedFile(String deviceId,
      String measurementId, TSDataType dataType, Map<String, String> props) {
    return null;
  }
}
