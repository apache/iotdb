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

package org.apache.iotdb.db.mpp.common.schematree.visitor;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.metadata.schemaregion.SchemaEngineMode;
import org.apache.iotdb.db.mpp.common.schematree.node.SchemaNode;

import static org.apache.iotdb.db.metadata.MetadataConstant.ALL_MATCH_PATTERN;

/**
 * Generate corresponding SchemaTreeVisitor for different types of schema regions, such as Memory,
 * Schema_File, Rocksdb_based, and Tag
 */
public class SchemaTreeVisitorFactory {

  private SchemaTreeVisitorFactory() {}

  public static SchemaTreeVisitorFactory getInstance() {
    return SchemaTreeVisitorFactoryHolder.INSTANCE;
  }

  /**
   * generate corresponding SchemaTreeDeviceVisitor for different types of schema regions
   *
   * @param schemaNode schema node
   * @param pathPattern can be a pattern or a full path of timeseries.
   * @param isPrefixMatch
   * @return schemaTreeDeviceVisitor
   */
  public SchemaTreeDeviceVisitor getSchemaTreeDeviceVisitor(
      SchemaNode schemaNode, PartialPath pathPattern, boolean isPrefixMatch) {
    SchemaTreeDeviceVisitor visitor;
    switch (SchemaEngineMode.valueOf(
        IoTDBDescriptor.getInstance().getConfig().getSchemaEngineMode())) {
      case Memory:
      case Schema_File:
        visitor = new SchemaTreeDeviceVisitor(schemaNode, pathPattern, isPrefixMatch);
        break;
      default:
        visitor = new SchemaTreeDeviceVisitor(schemaNode, ALL_MATCH_PATTERN, isPrefixMatch);
        break;
    }
    return visitor;
  }

  /**
   * generate corresponding SchemaTreeMeasurementVisitor for different types of schema regions
   *
   * @param schemaNode schema node
   * @param pathPattern can be a pattern or a full path of timeseries.
   * @return schemaTreeMeasurementVisitor
   */
  public SchemaTreeMeasurementVisitor getSchemaTreeMeasurementVisitor(
      SchemaNode schemaNode, PartialPath pathPattern) {
    SchemaTreeMeasurementVisitor visitor;
    switch (SchemaEngineMode.valueOf(
        IoTDBDescriptor.getInstance().getConfig().getSchemaEngineMode())) {
      case Memory:
      case Schema_File:
        visitor = generateSchemaTreeMeasurementVisitor(schemaNode, pathPattern);
        break;
      case Tag:
        if (pathPattern.getFullPath().contains(".**")) {
          visitor =
              generateSchemaTreeMeasurementVisitor(
                  schemaNode, ALL_MATCH_PATTERN.concatNode(pathPattern.getMeasurement()));
        } else {
          visitor = generateSchemaTreeMeasurementVisitor(schemaNode, pathPattern);
        }
        break;
      default:
        visitor = generateSchemaTreeMeasurementVisitor(schemaNode, ALL_MATCH_PATTERN);
        break;
    }
    return visitor;
  }

  private SchemaTreeMeasurementVisitor generateSchemaTreeMeasurementVisitor(
      SchemaNode schemaNode, PartialPath pathPattern) {
    return new SchemaTreeMeasurementVisitor(
        schemaNode,
        pathPattern,
        IoTDBDescriptor.getInstance().getConfig().getMaxQueryDeduplicatedPathNum() + 1,
        0,
        false);
  }

  private static class SchemaTreeVisitorFactoryHolder {
    private static final SchemaTreeVisitorFactory INSTANCE = new SchemaTreeVisitorFactory();

    private SchemaTreeVisitorFactoryHolder() {}
  }
}
