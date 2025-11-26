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

package org.apache.iotdb.db.storageengine.dataregion.tsfile.evolution;

import java.io.IOException;
import java.io.InputStream;
import org.apache.iotdb.db.utils.io.StreamSerializable;
import org.apache.tsfile.utils.ReadWriteForEncodingUtils;

/**
 * A schema evolution operation that can be applied to a TableSchemaMap.
 */
public interface SchemaEvolution extends StreamSerializable {

  /**
   * Apply this schema evolution operation to the given metadata.
   *
   * @param schema the schema to apply the operation to
   */
  void applyTo(EvolvedSchema schema);

  SchemaEvolutionType getEvolutionType();

  enum SchemaEvolutionType {
    TABLE_RENAME,
    COLUMN_RENAME
  }

  static SchemaEvolution createFrom(InputStream stream) throws IOException {
    int type = ReadWriteForEncodingUtils.readVarInt(stream);
    if (type < 0 || type > SchemaEvolutionType.values().length) {
      throw new IOException("Invalid evolution type: " + type);
    }
    SchemaEvolution evolution = null;
    SchemaEvolutionType evolutionType = SchemaEvolutionType.values()[type];
    switch (evolutionType) {
      case TABLE_RENAME:
        evolution = new TableRename();
        break;
      case COLUMN_RENAME:
        evolution = new ColumnRename();
        break;
      default:
        throw new IOException("Invalid evolution type: " + evolutionType);
    }
    evolution.deserialize(stream);
    return evolution;
  }
}