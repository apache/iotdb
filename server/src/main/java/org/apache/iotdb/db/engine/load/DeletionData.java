package org.apache.iotdb.db.engine.load;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.engine.modification.Deletion;
import org.apache.iotdb.db.engine.modification.ModificationFile;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class DeletionData implements TsFileData {
  private final Deletion deletion;

  public DeletionData(Deletion deletion) {
    this.deletion = deletion;
  }

  @Override
  public long getDataSize() {
    return 0;
  }

  @Override
  public void writeToFileWriter(TsFileIOWriter writer) throws IOException {
    File tsFile = writer.getFile();
    try (ModificationFile modificationFile =
        new ModificationFile(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX)) {
      deletion.setFileOffset(tsFile.length());
      modificationFile.write(deletion);
    }
  }

  @Override
  public boolean isModification() {
    return true;
  }

  @Override
  public void serialize(DataOutputStream stream, File tsFile) throws IOException {
    ReadWriteIOUtils.write(isModification(), stream);
    deletion.serializeWithoutFileOffset(stream);
  }

  public static DeletionData deserialize(InputStream stream)
      throws IllegalPathException, IOException {
    return new DeletionData(Deletion.deserializeWithoutFileOffset(new DataInputStream(stream)));
  }
}
