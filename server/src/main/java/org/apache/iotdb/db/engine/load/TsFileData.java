package org.apache.iotdb.db.engine.load;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.tsfile.exception.write.PageException;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public interface TsFileData {
  long getDataSize();

  void writeToFileWriter(TsFileIOWriter writer) throws IOException;

  boolean isModification();

  void serialize(DataOutputStream stream, File tsFile) throws IOException;

  static TsFileData deserialize(InputStream stream)
      throws IOException, PageException, IllegalPathException {
    boolean isModification = ReadWriteIOUtils.readBool(stream);
    return isModification ? DeletionData.deserialize(stream) : ChunkData.deserialize(stream);
  }
}
