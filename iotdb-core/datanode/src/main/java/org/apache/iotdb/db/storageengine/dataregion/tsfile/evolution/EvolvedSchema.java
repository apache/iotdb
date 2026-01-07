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

import org.apache.iotdb.db.storageengine.dataregion.modification.DeletionPredicate;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.ModEntry.ModType;
import org.apache.iotdb.db.storageengine.dataregion.modification.TableDeletionEntry;
import org.apache.iotdb.db.storageengine.dataregion.modification.TagPredicate;

import org.apache.tsfile.enums.ColumnCategory;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.IDeviceID.Factory;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.file.metadata.TimeseriesMetadata;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;
import org.apache.tsfile.write.schema.Schema;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.function.Function;
import java.util.stream.Collectors;

public class EvolvedSchema {
  // the evolved table names after applying all schema evolution operations
  private Map<String, String> finalToOriginalTableNames = new LinkedHashMap<>();

  /**
   * the first key is the evolved table name, the second key is the evolved column name, and the
   * value is the original column name before any schema evolution.
   */
  private Map<String, Map<String, String>> finalToOriginalColumnNames = new LinkedHashMap<>();

  // the reversed version of finalToOriginalTableNames
  private Map<String, String> originalToFinalTableNames = new LinkedHashMap<>();

  // the reversed version of finalToOriginalColumnNames
  private Map<String, Map<String, String>> originalToFinalColumnNames = new LinkedHashMap<>();

  public void renameTable(String oldTableName, String newTableName) {
    if (!finalToOriginalTableNames.containsKey(oldTableName)
        || finalToOriginalTableNames.get(oldTableName).isEmpty()) {
      finalToOriginalTableNames.put(newTableName, oldTableName);
      finalToOriginalTableNames.put(oldTableName, "");
      originalToFinalTableNames.put(oldTableName, newTableName);
    } else {
      // mark the old table name as non-exists (empty)
      String originalName = finalToOriginalTableNames.put(oldTableName, "");
      finalToOriginalTableNames.put(newTableName, originalName);
      originalToFinalTableNames.put(originalName, newTableName);
    }

    if (finalToOriginalColumnNames.containsKey(oldTableName)) {
      Map<String, String> columnMap = finalToOriginalColumnNames.remove(oldTableName);
      finalToOriginalColumnNames.put(newTableName, columnMap);
    }
  }

  public void renameColumn(String newTableName, String oldColumnName, String newColumnName) {
    Map<String, String> columnNameMap =
        finalToOriginalColumnNames.computeIfAbsent(newTableName, t -> new LinkedHashMap<>());
    String originalTableName = getOriginalTableName(newTableName);
    if (!columnNameMap.containsKey(oldColumnName) || columnNameMap.get(oldColumnName).isEmpty()) {
      columnNameMap.put(newColumnName, oldColumnName);
      columnNameMap.put(oldColumnName, "");
      originalToFinalColumnNames
          .computeIfAbsent(originalTableName, t -> new LinkedHashMap<>())
          .put(oldColumnName, newColumnName);
    } else {
      // mark the old column name as non-exists
      String originalName = columnNameMap.put(oldColumnName, "");
      columnNameMap.put(newColumnName, originalName);
      originalToFinalColumnNames
          .computeIfAbsent(originalTableName, t -> new LinkedHashMap<>())
          .put(originalName, newColumnName);
    }
  }

  public String getOriginalTableName(String finalTableName) {
    return finalToOriginalTableNames.getOrDefault(finalTableName, finalTableName);
  }

  public String getFinalTableName(String originalTableName) {
    return originalToFinalTableNames.getOrDefault(originalTableName, originalTableName);
  }

  public String getOriginalColumnName(String tableName, String evolvedColumnName) {
    Map<String, String> columnNameMap = finalToOriginalColumnNames.get(tableName);
    if (columnNameMap == null) {
      return evolvedColumnName;
    }
    return columnNameMap.getOrDefault(evolvedColumnName, evolvedColumnName);
  }

  public String getFinalColumnName(String originalTableName, String originalColumnName) {
    return originalToFinalColumnNames
        .getOrDefault(originalTableName, Collections.emptyMap())
        .getOrDefault(originalColumnName, originalColumnName);
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    EvolvedSchema that = (EvolvedSchema) o;
    return Objects.equals(finalToOriginalTableNames, that.finalToOriginalTableNames)
        && Objects.equals(finalToOriginalColumnNames, that.finalToOriginalColumnNames);
  }

  @Override
  public int hashCode() {
    return Objects.hash(finalToOriginalTableNames, finalToOriginalColumnNames);
  }

  @Override
  public String toString() {
    return "EvolvedSchema{"
        + "originalTableNames="
        + finalToOriginalTableNames
        + ", originalColumnNames="
        + finalToOriginalColumnNames
        + '}';
  }

  public List<SchemaEvolution> toSchemaEvolutions() {
    List<SchemaEvolution> schemaEvolutions = new ArrayList<>();
    finalToOriginalTableNames.forEach(
        (finalTableName, originalTableName) -> {
          if (!originalTableName.isEmpty()) {
            schemaEvolutions.add(new TableRename(originalTableName, finalTableName));
          }
        });
    finalToOriginalColumnNames.forEach(
        (finalTableName, originalColumnNameMap) -> {
          originalColumnNameMap.forEach(
              (finalColumnName, originalColumnName) -> {
                if (!originalColumnName.isEmpty()) {
                  schemaEvolutions.add(
                      new ColumnRename(finalTableName, originalColumnName, finalColumnName, null));
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

  public ModEntry rewriteToFinal(ModEntry entry) {
    if (entry.getType() == ModType.TABLE_DELETION) {
      return rewriteToFinal(((TableDeletionEntry) entry));
    }
    return entry;
  }

  public TableDeletionEntry rewriteToOriginal(TableDeletionEntry entry) {
    DeletionPredicate deletionPredicate = rewriteToOriginal(entry.getPredicate());
    return new TableDeletionEntry(deletionPredicate, entry.getTimeRange());
  }

  public TableDeletionEntry rewriteToFinal(TableDeletionEntry entry) {
    DeletionPredicate deletionPredicate = rewriteToFinal(entry.getPredicate());
    return new TableDeletionEntry(deletionPredicate, entry.getTimeRange());
  }

  private DeletionPredicate rewriteToFinal(DeletionPredicate predicate) {
    String finalTableName = getFinalTableName(predicate.getTableName());
    TagPredicate tagPredicate = predicate.getTagPredicate();
    tagPredicate = tagPredicate.rewriteToOriginal(this);
    List<String> newMeasurements =
        predicate.getMeasurementNames().stream()
            .map(m -> getFinalColumnName(predicate.getTableName(), m))
            .collect(Collectors.toList());
    return new DeletionPredicate(finalTableName, tagPredicate, newMeasurements);
  }

  private DeletionPredicate rewriteToOriginal(DeletionPredicate predicate) {
    String originalTableName = getOriginalTableName(predicate.getTableName());
    TagPredicate tagPredicate = predicate.getTagPredicate();
    tagPredicate = tagPredicate.rewriteToOriginal(this);
    List<String> newMeasurements =
        predicate.getMeasurementNames().stream()
            .map(m -> getOriginalColumnName(predicate.getTableName(), m))
            .collect(Collectors.toList());
    return new DeletionPredicate(originalTableName, tagPredicate, newMeasurements);
  }

  public IDeviceID rewriteToOriginal(IDeviceID deviceID) {
    String tableName = deviceID.getTableName();
    String originalTableName = getOriginalTableName(tableName);
    return rewriteTableName(deviceID, originalTableName);
  }

  public IDeviceID rewriteToFinal(IDeviceID deviceID) {
    String tableName = deviceID.getTableName();
    String finalTableName = getFinalTableName(tableName);
    return rewriteTableName(deviceID, finalTableName);
  }

  public void rewriteToFinal(
      String originalTableName, List<TimeseriesMetadata> timeseriesMetadataList) {
    timeseriesMetadataList.forEach(
        timeseriesMetadata -> {
          String finalColumnName =
              getFinalColumnName(originalTableName, timeseriesMetadata.getMeasurementId());
          timeseriesMetadata.setMeasurementId(finalColumnName);
        });
  }

  public Map<String, TableSchema> rewriteToFinal(Map<String, TableSchema> tableSchemas) {
    Map<String, TableSchema> finalTableSchemas = new HashMap<>(tableSchemas.size());
    for (Map.Entry<String, TableSchema> entry : tableSchemas.entrySet()) {
      TableSchema tableSchema = entry.getValue();
      tableSchema = rewriteToFinal(tableSchema);
      finalTableSchemas.put(tableSchema.getTableName(), tableSchema);
    }
    return finalTableSchemas;
  }

  private TableSchema rewriteToOriginal(TableSchema tableSchema) {
    String originalTableName = getOriginalTableName(tableSchema.getTableName());

    List<IMeasurementSchema> measurementSchemas =
        new ArrayList<>(tableSchema.getColumnSchemas().size());
    List<ColumnCategory> columnCategories = new ArrayList<>(tableSchema.getColumnTypes().size());
    List<IMeasurementSchema> columnSchemas = tableSchema.getColumnSchemas();
    for (int i = 0, columnSchemasSize = columnSchemas.size(); i < columnSchemasSize; i++) {
      IMeasurementSchema measurementSchema = columnSchemas.get(i);
      measurementSchemas.add(
          new MeasurementSchema(
              getOriginalColumnName(
                  tableSchema.getTableName(), measurementSchema.getMeasurementName()),
              measurementSchema.getType(),
              measurementSchema.getEncodingType(),
              measurementSchema.getCompressor()));
      columnCategories.add(tableSchema.getColumnTypes().get(i));
    }

    TableSchema schema = new TableSchema(originalTableName, measurementSchemas,
        columnCategories);
    schema.setUpdatable(tableSchema.isUpdatable());
    return schema;
  }

  public TableSchema rewriteToFinal(TableSchema tableSchema) {
    String finalTableName = getFinalTableName(tableSchema.getTableName());

    List<IMeasurementSchema> measurementSchemas =
        new ArrayList<>(tableSchema.getColumnSchemas().size());
    List<ColumnCategory> columnCategories = new ArrayList<>(tableSchema.getColumnTypes().size());
    List<IMeasurementSchema> columnSchemas = tableSchema.getColumnSchemas();
    for (int i = 0, columnSchemasSize = columnSchemas.size(); i < columnSchemasSize; i++) {
      IMeasurementSchema measurementSchema = columnSchemas.get(i);
      measurementSchemas.add(
          new MeasurementSchema(
              getFinalColumnName(
                      tableSchema.getTableName(), measurementSchema.getMeasurementName()),
                  measurementSchema.getType(),
              measurementSchema.getEncodingType(), measurementSchema.getCompressor()));
      columnCategories.add(tableSchema.getColumnTypes().get(i));
    }

    TableSchema schema = new TableSchema(finalTableName, measurementSchemas,
        columnCategories);
    schema.setUpdatable(tableSchema.isUpdatable());
    return schema;
  }

  @SuppressWarnings("SuspiciousSystemArraycopy")
  public static IDeviceID rewriteTableName(IDeviceID deviceID, String newTableName) {
    String tableName = deviceID.getTableName();
    if (!tableName.equals(newTableName)) {
      Object[] segments = deviceID.getSegments();
      String[] newSegments = new String[segments.length];
      newSegments[0] = newTableName;
      System.arraycopy(segments, 1, newSegments, 1, segments.length - 1);
      return Factory.DEFAULT_FACTORY.create(newSegments);
    }
    return deviceID;
  }

  public static EvolvedSchema deepCopy(EvolvedSchema evolvedSchema) {
    EvolvedSchema newEvolvedSchema = new EvolvedSchema();
    newEvolvedSchema.finalToOriginalTableNames =
        new LinkedHashMap<>(evolvedSchema.finalToOriginalTableNames);
    newEvolvedSchema.finalToOriginalColumnNames =
        new LinkedHashMap<>(evolvedSchema.finalToOriginalColumnNames);
    newEvolvedSchema.originalToFinalTableNames =
        new LinkedHashMap<>(evolvedSchema.originalToFinalTableNames);
    newEvolvedSchema.originalToFinalColumnNames =
        new LinkedHashMap<>(evolvedSchema.originalToFinalColumnNames);
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
    if (i == schemas.length) {
      return firstNotNullSchema;
    }

    if (firstNotNullSchema == null) {
      return null;
    }
    EvolvedSchema mergedSchema = deepCopy(firstNotNullSchema);

    for (; i < schemas.length; i++) {
      if (schemas[i] != null) {
        EvolvedSchema newSchema = schemas[i];
        for (Entry<String, String> finalOriginalTableName :
            newSchema.finalToOriginalTableNames.entrySet()) {
          if (!finalOriginalTableName.getValue().isEmpty()) {
            mergedSchema.renameTable(
                finalOriginalTableName.getValue(), finalOriginalTableName.getKey());
          }
        }
        for (Entry<String, Map<String, String>> finalTableNameColumnNameMapEntry :
            newSchema.finalToOriginalColumnNames.entrySet()) {
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

  public Schema rewriteToOriginal(Schema schema) {
    return rewriteToOriginal(schema, null);
  }

  public Schema rewriteToOriginal(
      Schema schema, Function<TableSchema, TableSchema> tableSchemaTransformer) {
    Schema copySchema = new Schema();
    for (TableSchema tableSchema : schema.getTableSchemaMap().values()) {
      TableSchema originalSchema = rewriteToOriginal(tableSchema);
      if (tableSchemaTransformer != null) {
        originalSchema = tableSchemaTransformer.apply(originalSchema);
      }
      copySchema.registerTableSchema(originalSchema);
    }
    return copySchema;
  }
}
