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
package org.apache.iotdb.db.metadata.schemaRegion;

import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.CreateAlignedTimeSeriesPlanImpl;
import org.apache.iotdb.db.metadata.plan.schemaregion.impl.CreateTimeSeriesPlanImpl;
import org.apache.iotdb.db.metadata.schemaregion.ISchemaRegion;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import java.util.List;
import java.util.Map;

public class SchemaRegionTestUtil {

  public static void createTimeseries(
      ISchemaRegion schemaRegion,
      String fullPath,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      Map<String, String> tags,
      Map<String, String> attributes,
      String alias)
      throws MetadataException {
    schemaRegion.createTimeseries(
        new CreateTimeSeriesPlanImpl(
            new PartialPath(fullPath),
            dataType,
            encoding,
            compressor,
            props,
            tags,
            attributes,
            alias),
        -1);
  }

  public static void createTimeseries(
      ISchemaRegion schemaRegion,
      List<String> fullPaths,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<Map<String, String>> props,
      List<Map<String, String>> tags,
      List<Map<String, String>> attributes,
      List<String> alias)
      throws MetadataException {
    for (int i = 0; i < fullPaths.size(); i++) {
      schemaRegion.createTimeseries(
          new CreateTimeSeriesPlanImpl(
              new PartialPath(fullPaths.get(i)),
              dataTypes.get(i),
              encodings.get(i),
              compressors.get(i),
              props == null ? null : props.get(i),
              tags == null ? null : tags.get(i),
              attributes == null ? null : attributes.get(i),
              alias == null ? null : alias.get(i)),
          -1);
    }
  }

  public static void createAlignedTimeseries(
      ISchemaRegion schemaRegion,
      String devicePath,
      List<String> measurements,
      List<TSDataType> dataTypes,
      List<TSEncoding> encodings,
      List<CompressionType> compressors,
      List<Map<String, String>> tags,
      List<Map<String, String>> attributes,
      List<String> alias)
      throws MetadataException {
    schemaRegion.createAlignedTimeSeries(
        new CreateAlignedTimeSeriesPlanImpl(
            new PartialPath(devicePath),
            measurements,
            dataTypes,
            encodings,
            compressors,
            alias,
            tags,
            attributes));
  }
}
