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

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;
import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry.ModType;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TagPredicate;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;

public class EvolvedSchema {
  // the evolved table names after applying all schema evolution operations
  private Map<String, String> originalTableNames = new LinkedHashMap<>();

  /**
   * the first key is the evolved table name, the second key is the evolved column name, and the
   * value is the original column name before any schema evolution.
   */
  private Map<String, Map<String, String>> originalColumnNames = new LinkedHashMap<>();

  public void renameTable(String oldTableName, String newTableName) {
    if (!originalTableNames.containsKey(oldTableName) || originalTableNames.get(oldTableName).isEmpty()) {
      originalTableNames.put(newTableName, oldTableName);
      originalTableNames.put(oldTableName, "");
    } else {
      // mark the old table name as non-exists
      String originalName = originalTableNames.put(oldTableName, "");
      originalTableNames.put(newTableName, originalName);
    }

    if (originalColumnNames.containsKey(oldTableName)) {
      Map<String, String> columnMap = originalColumnNames.remove(oldTableName);
      originalColumnNames.put(newTableName, columnMap);
    }
  }

  public void renameColumn(String tableName, String oldColumnName, String newColumnName) {
    Map<String, String> columnNameMap =
        originalColumnNames.computeIfAbsent(tableName, t -> new LinkedHashMap<>());
    if (!columnNameMap.containsKey(oldColumnName) || columnNameMap.get(oldColumnName).isEmpty()) {
      columnNameMap.put(newColumnName, oldColumnName);
      columnNameMap.put(oldColumnName, "");
    } else {
      // mark the old column name as non-exists
      String originalName = columnNameMap.put(oldColumnName, "");
      columnNameMap.put(newColumnName, originalName);
    }
  }

  public String getOriginalTableName(String evolvedTableName) {
    return originalTableNames.getOrDefault(evolvedTableName, evolvedTableName);
  }

  public String getOriginalColumnName(String tableName, String evolvedColumnName) {
    Map<String, String> columnNameMap = originalColumnNames.get(tableName);
    if (columnNameMap == null) {
      return evolvedColumnName;
    }
    return columnNameMap.getOrDefault(evolvedColumnName, evolvedColumnName);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EvolvedSchema that = (EvolvedSchema) o;
    return Objects.equals(originalTableNames, that.originalTableNames)
        && Objects.equals(originalColumnNames, that.originalColumnNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(originalTableNames, originalColumnNames);
  }

  @Override
  public String toString() {
    return "EvolvedSchema{"
        + "originalTableNames="
        + originalTableNames
        + ", originalColumnNames="
        + originalColumnNames
        + '}';
  }

  public List<SchemaEvolution> toSchemaEvolutions() {
      List<SchemaEvolution> schemaEvolutions = new ArrayList<>();
      originalTableNames.forEach((finalTableName, originalTableName) -> {
        if (!originalTableName.isEmpty()) {
          schemaEvolutions.add(new TableRename(originalTableName, finalTableName));
        }
      });
      originalColumnNames.forEach((finalTableName, originalColumnNameMap) -> {
        originalColumnNameMap.forEach((finalColumnName, originalColumnName) -> {
          if (!originalColumnName.isEmpty()) {
            schemaEvolutions.add(new ColumnRename(finalTableName, originalColumnName, finalColumnName, null));
          }
        });
      });
      return schemaEvolutions;
  }

  public ModEntry rewriteToOriginal(ModEntry entry) {
    if (entry.getType() == ModType.TABLE_DELETION) {
      return rewriteToOriginal(((TableDeletionEntry) entry));
    }
    return entry;
  }

  public TableDeletionEntry rewriteToOriginal(TableDeletionEntry entry) {
    DeletionPredicate deletionPredicate = rewriteToOriginal(entry.getPredicate());
    return new TableDeletionEntry(deletionPredicate, entry.getTimeRange());
  }

  private DeletionPredicate rewriteToOriginal(DeletionPredicate predicate) {
    String originalTableName = getOriginalTableName(predicate.getTableName());
    TagPredicate tagPredicate = predicate.getTagPredicate();
    tagPredicate = tagPredicate.rewriteToOriginal(this);
    List<String> newMeasurements =
    predicate.getMeasurementNames().stream().map(m -> getOriginalColumnName(predicate.getTableName(), m)).collect(
        Collectors.toList());
    return new DeletionPredicate(originalTableName, tagPredicate, newMeasurements);
  }

  public IDeviceID rewriteToOriginal(IDeviceID deviceID) {
    String tableName = deviceID.getTableName();
    String originalTableName = getOriginalTableName(tableName);
    return rewriteToOriginal(deviceID, originalTableName);
  }

  @SuppressWarnings("SuspiciousSystemArraycopy")
  public static IDeviceID rewriteToOriginal(IDeviceID deviceID, String originalTableName) {
    String tableName = deviceID.getTableName();
    if (!tableName.equals(originalTableName)) {
      Object[] segments = deviceID.getSegments();
      String[] newSegments = new String[segments.length];
      newSegments[0] = originalTableName;
      System.arraycopy(segments, 1, newSegments, 1, segments.length - 1);
      return Factory.DEFAULT_FACTORY.create(newSegments);
    }
    return deviceID;
  }

  public static EvolvedSchema deepCopy(EvolvedSchema evolvedSchema) {
    EvolvedSchema newEvolvedSchema = new EvolvedSchema();
    newEvolvedSchema.originalTableNames = new LinkedHashMap<>(evolvedSchema.originalTableNames);
    newEvolvedSchema.originalColumnNames = new LinkedHashMap<>(evolvedSchema.originalColumnNames);
    return newEvolvedSchema;
  }

  public static EvolvedSchema merge(EvolvedSchema... schemas) {
    EvolvedSchema firstNotNullSchema = null;
    int i = 0;
    for (; i < schemas.length; i++) {
      if (schemas[i] != null) {
        firstNotNullSchema = schemas[i];
        i++;
        break;
      }
    }

    if (firstNotNullSchema == null) {
      return null;
    }
    EvolvedSchema mergedSchema = deepCopy(firstNotNullSchema);

    for (; i < schemas.length; i++) {
      if (schemas[i] != null) {
        EvolvedSchema newSchema = schemas[i];
        for (Entry<String, String> finalOriginalTableName :
            newSchema.originalTableNames.entrySet()) {
          if (!finalOriginalTableName.getValue().isEmpty()) {
            mergedSchema.renameTable(
                finalOriginalTableName.getValue(), finalOriginalTableName.getKey());
          }
        }
        for (Entry<String, Map<String, String>> finalTableNameColumnNameMapEntry :
            newSchema.originalColumnNames.entrySet()) {
          for (Entry<String, String> finalColNameOriginalColNameEntry :
              finalTableNameColumnNameMapEntry.getValue().entrySet()) {
            if (!finalColNameOriginalColNameEntry.getValue().isEmpty()) {
              String finalTableName = finalTableNameColumnNameMapEntry.getKey();
              String finalColName = finalColNameOriginalColNameEntry.getKey();
              String originalColName = finalColNameOriginalColNameEntry.getValue();
              mergedSchema.renameColumn(finalTableName, originalColName, finalColName);
            }
          }
        }
      }
    }
    return mergedSchema;
  }
}
