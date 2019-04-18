package org.apache.iotdb.db.engine.overflow;

import java.io.IOException;
import org.apache.iotdb.db.engine.bufferwrite.Action;
import org.apache.iotdb.db.engine.tsfiledata.TsFileProcessor;
import org.apache.iotdb.db.engine.version.VersionController;
import org.apache.iotdb.db.exception.BufferWriteProcessorException;
import org.apache.iotdb.tsfile.write.schema.FileSchema;

public class OverflowProcessor extends TsFileProcessor {

  /**
   * constructor of BufferWriteProcessor. data will be stored in baseDir/processorName/ folder.
   *
   * @param processorName processor name
   * @param fileSchemaRef file schema
   * @throws BufferWriteProcessorException BufferWriteProcessorException
   */
  public OverflowProcessor(String processorName,
      Action beforeFlushAction,
      Action afterFlushAction,
      Action afterCloseAction,
      VersionController versionController,
      FileSchema fileSchemaRef)
      throws BufferWriteProcessorException, IOException {
    super(processorName, beforeFlushAction, afterFlushAction, afterCloseAction, versionController,
        fileSchemaRef);
  }
}
