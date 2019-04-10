/**
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
package org.apache.iotdb.db.qp.logical.sys;

import org.apache.iotdb.db.qp.logical.RootOperator;
import org.apache.iotdb.tsfile.read.common.Path;

/**
 * this class maintains information in Metadata.namespace statement
 */
public class PropertyOperator extends RootOperator {

  private final PropertyType propertyType;
  private Path propertyPath;
  private Path metadataPath;
  public PropertyOperator(int tokenIntType, PropertyType type) {
    super(tokenIntType);
    propertyType = type;
    operatorType = OperatorType.PROPERTY;
  }

  public Path getPropertyPath() {
    return propertyPath;
  }

  public void setPropertyPath(Path propertyPath) {
    this.propertyPath = propertyPath;
  }

  public Path getMetadataPath() {
    return metadataPath;
  }

  public void setMetadataPath(Path metadataPath) {
    this.metadataPath = metadataPath;
  }

  public PropertyType getPropertyType() {
    return propertyType;
  }

  public enum PropertyType {
    ADD_TREE, ADD_PROPERTY_LABEL, DELETE_PROPERTY_LABEL, ADD_PROPERTY_TO_METADATA, DEL_PROPERTY_FROM_METADATA;

    /**
     * deserialize short number.
     *
     * @param i short number
     * @return NamespaceType
     */
    public static PropertyType deserialize(short i) {
      switch (i) {
        case 0:
          return ADD_TREE;
        case 1:
          return ADD_PROPERTY_LABEL;
        case 2:
          return DELETE_PROPERTY_LABEL;
        case 3:
          return ADD_PROPERTY_TO_METADATA;
        case 4:
          return DEL_PROPERTY_FROM_METADATA;
        default:
          return null;
      }
    }

    /**
     * serialize.
     *
     * @return short number
     */
    public short serialize() {
      switch (this) {
        case ADD_TREE:
          return 0;
        case ADD_PROPERTY_LABEL:
          return 1;
        case DELETE_PROPERTY_LABEL:
          return 2;
        case ADD_PROPERTY_TO_METADATA:
          return 3;
        case DEL_PROPERTY_FROM_METADATA:
          return 4;
        default:
          return -1;
      }
    }
  }
}
