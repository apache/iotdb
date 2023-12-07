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

package org.apache.iotdb.db.queryengine.execution.load;

import org.apache.iotdb.commons.exception.IllegalPathException;
import org.apache.iotdb.db.storageengine.dataregion.modification.Deletion;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModificationFile;
import org.apache.iotdb.tsfile.utils.ReadWriteIOUtils;
import org.apache.iotdb.tsfile.write.writer.TsFileIOWriter;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;

public class DeletionData implements TsFileData {
  private final Deletion deletion;
  private int splitId;

  public DeletionData(Deletion deletion) {
    this.deletion = deletion;
  }

  @Override
  public long getDataSize() {
    return Long.BYTES;
  }

  @Override
  public void writeToFileWriter(TsFileIOWriter writer) throws IOException {
    File tsFile = writer.getFile();
    try (ModificationFile modificationFile =
        new ModificationFile(tsFile.getAbsolutePath() + ModificationFile.FILE_SUFFIX)) {
      writer.flush();
      deletion.setFileOffset(tsFile.length());
      modificationFile.write(deletion);
    }
  }

  @Override
  public boolean isModification() {
    return true;
  }

  @Override
  public void serialize(DataOutputStream stream) throws IOException {
    ReadWriteIOUtils.write(isModification(), stream);
    deletion.serializeWithoutFileOffset(stream);
    ReadWriteIOUtils.write(splitId, stream);
  }

  public static DeletionData deserialize(InputStream stream)
      throws IllegalPathException, IOException {
    DataInputStream dataInputStream = new DataInputStream(stream);
    DeletionData deletionData =
        new DeletionData(Deletion.deserializeWithoutFileOffset(dataInputStream));
    deletionData.setSplitId(dataInputStream.readInt());
    return deletionData;
  }

  @Override
  public int getSplitId() {
    return splitId;
  }

  @Override
  public void setSplitId(int sid) {
    this.splitId = sid;
  }

  @Override
  public String toString() {
    return "DeletionData{" + "deletion=" + deletion + ", splitId=" + splitId + '}';
  }

  public Deletion getDeletion() {
    return deletion;
  }
}
