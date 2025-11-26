/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import org.apache.tsfile.file.metadata.TsFileMetadata;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;
import org.apache.tsfile.utils.ReadWriteIOUtils;

/**
 * A schema evolution operation that renames a column in a table schema.
 */
public class ColumnRename implements SchemaEvolution {

  private String tableName;
  private String nameBefore;
  private String nameAfter;

  // for deserialization
  public ColumnRename() {
  }

  public ColumnRename(String tableName, String nameBefore, String nameAfter) {
    this.tableName = tableName.toLowerCase();
    this.nameBefore = nameBefore.toLowerCase();
    this.nameAfter = nameAfter.toLowerCase();
  }

  @Override
  public SchemaEvolutionType getEvolutionType() {
    return SchemaEvolutionType.COLUMN_RENAME;
  }

  @Override
  public void applyTo(EvolvedSchema evolvedSchema) {
    evolvedSchema.renameColumn(tableName, nameBefore, nameAfter);
  }

  @Override
  public long serialize(OutputStream stream) throws IOException {
    int size = ReadWriteForEncodingUtils.writeVarInt(getEvolutionType().ordinal(), stream);
    size += ReadWriteIOUtils.writeVar(tableName, stream);
    size += ReadWriteIOUtils.writeVar(nameBefore, stream);
    size += ReadWriteIOUtils.writeVar(nameAfter, stream);
    return size;
  }

  @Override
  public void deserialize(InputStream stream) throws IOException {
    tableName = ReadWriteIOUtils.readVarIntString(stream);
    nameBefore = ReadWriteIOUtils.readVarIntString(stream);
    nameAfter = ReadWriteIOUtils.readVarIntString(stream);
  }
}
