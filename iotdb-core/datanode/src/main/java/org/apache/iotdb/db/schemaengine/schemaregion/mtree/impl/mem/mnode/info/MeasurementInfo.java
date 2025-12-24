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
package org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.info;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.info.IMeasurementInfo;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;

import org.apache.tsfile.enums.TSDataType;
import org.apache.tsfile.write.schema.IMeasurementSchema;
import org.apache.tsfile.write.schema.MeasurementSchema;

import java.util.Map;

public class MeasurementInfo implements IMeasurementInfo {

  // Props keys for alias series feature
  /**
   * Key for IS_RENAMED property in schema props (value: "true" or "false") Indicates this is an
   * alias series (renaming completed)
   */
  public static final String IS_RENAMED_KEY = "IS_RENAMED";

  /**
   * Key for IS_RENAMING property in schema props (value: "true" or "false") Indicates this
   * measurement is currently being renamed (for non-procedure alter operations)
   */
  public static final String IS_RENAMING_KEY = "IS_RENAMING";

  /** Key for DISABLED property in schema props (value: "true" or "false") */
  public static final String DISABLED_KEY = "DISABLED";

  /**
   * Key for ORIGINAL_PATH property in schema props (value: physical path string) If this key
   * exists, it indicates this is an alias series pointing to the physical path
   */
  public static final String ORIGINAL_PATH_KEY = "ORIGINAL_PATH";

  /**
   * Key for ALIAS_PATH property in schema props (value: alias path string) If this key exists, it
   * indicates this physical series has an alias pointing to it
   */
  public static final String ALIAS_PATH_KEY = "ALIAS_PATH";

  /** alias name of this measurement */
  protected String alias;

  /** tag/attribute's start offset in tag file */
  private long offset = -1;

  /** measurement's Schema for one timeseries represented by current leaf node */
  private IMeasurementSchema schema;

  /** whether this measurement is pre deleted and considered in black list */
  private boolean preDeleted = false;

  // alias length, hashCode and occupation in aliasMap, 4 + 4 + 44 = 52B
  private static final int ALIAS_BASE_SIZE = 52;

  public MeasurementInfo(IMeasurementSchema schema, String alias) {
    this.schema = schema;
    this.alias = alias;
  }

  @Override
  public void moveDataToNewMNode(IMeasurementMNode<?> newMNode) {
    newMNode.setSchema(schema);
    newMNode.setAlias(alias);
    newMNode.setOffset(offset);
    newMNode.setPreDeleted(preDeleted);
    // Copy alias series properties
    newMNode.setIsRenamed(this.isRenamed());
    newMNode.setIsRenaming(this.isRenaming());
    newMNode.setDisabled(this.isDisabled());
    newMNode.setOriginalPath(this.getOriginalPath());
    newMNode.setAliasPath(this.getAliasPath());
  }

  @Override
  public IMeasurementSchema getSchema() {
    return schema;
  }

  @Override
  public void setSchema(IMeasurementSchema schema) {
    this.schema = schema;
  }

  @Override
  public TSDataType getDataType() {
    return schema.getType();
  }

  @Override
  public String getAlias() {
    return alias;
  }

  @Override
  public void setAlias(String alias) {
    this.alias = alias;
  }

  @Override
  public long getOffset() {
    return offset;
  }

  @Override
  public void setOffset(long offset) {
    this.offset = offset;
  }

  @Override
  public boolean isPreDeleted() {
    return preDeleted;
  }

  @Override
  public void setPreDeleted(boolean preDeleted) {
    this.preDeleted = preDeleted;
  }

  /** Get props from MeasurementSchema. Returns null if schema is not MeasurementSchema. */
  private Map<String, String> getProps() {
    if (schema instanceof MeasurementSchema) {
      return ((MeasurementSchema) schema).getProps();
    }
    return null;
  }

  /** Get or create props map from MeasurementSchema. */
  private Map<String, String> getOrCreateProps() {
    if (schema instanceof MeasurementSchema) {
      Map<String, String> props = ((MeasurementSchema) schema).getProps();
      if (props == null) {
        props = new java.util.HashMap<>();
        ((MeasurementSchema) schema).setProps(props);
      }
      return props;
    }
    throw new IllegalStateException("Schema is not MeasurementSchema, cannot access props");
  }

  @Override
  public boolean isRenamed() {
    Map<String, String> props = getProps();
    if (props == null) {
      return false;
    }
    String value = props.get(IS_RENAMED_KEY);
    return Boolean.parseBoolean(value);
  }

  @Override
  public void setIsRenamed(boolean isRenamed) {
    Map<String, String> props = getOrCreateProps();
    if (isRenamed) {
      props.put(IS_RENAMED_KEY, "true");
    } else {
      props.remove(IS_RENAMED_KEY);
    }
  }

  @Override
  public boolean isRenaming() {
    Map<String, String> props = getProps();
    if (props == null) {
      return false;
    }
    String value = props.get(IS_RENAMING_KEY);
    return Boolean.parseBoolean(value);
  }

  @Override
  public void setIsRenaming(boolean isRenaming) {
    Map<String, String> props = getOrCreateProps();
    if (isRenaming) {
      props.put(IS_RENAMING_KEY, "true");
    } else {
      props.remove(IS_RENAMING_KEY);
    }
  }

  @Override
  public boolean isDisabled() {
    Map<String, String> props = getProps();
    if (props == null) {
      return false;
    }
    String value = props.get(DISABLED_KEY);
    return Boolean.parseBoolean(value);
  }

  @Override
  public void setDisabled(boolean isDisabled) {
    Map<String, String> props = getOrCreateProps();
    props.put(DISABLED_KEY, String.valueOf(isDisabled));
  }

  @Override
  public PartialPath getOriginalPath() {
    Map<String, String> props = getProps();
    if (props == null) {
      return null;
    }
    String pathString = props.get(ORIGINAL_PATH_KEY);
    if (pathString == null) {
      return null;
    }
    try {
      return new PartialPath(pathString);
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public void setOriginalPath(PartialPath originalPath) {
    Map<String, String> props = getOrCreateProps();
    if (originalPath == null) {
      props.remove(ORIGINAL_PATH_KEY);
    } else {
      props.put(ORIGINAL_PATH_KEY, originalPath.getFullPath());
    }
  }

  @Override
  public PartialPath getAliasPath() {
    Map<String, String> props = getProps();
    if (props == null) {
      return null;
    }
    String pathString = props.get(ALIAS_PATH_KEY);
    if (pathString == null) {
      return null;
    }
    try {
      return new PartialPath(pathString);
    } catch (Exception e) {
      return null;
    }
  }

  @Override
  public void setAliasPath(PartialPath aliasPath) {
    Map<String, String> props = getOrCreateProps();
    if (aliasPath == null) {
      props.remove(ALIAS_PATH_KEY);
    } else {
      props.put(ALIAS_PATH_KEY, aliasPath.getFullPath());
    }
  }

  /**
   * The memory occupied by an MeasurementInfo based occupation
   *
   * <ol>
   *   <li>object header, 8B
   *   <li>alias reference, 8B
   *   <li>long tagOffset, 8B
   *   <li>boolean preDeleted, 1B
   *   <li>estimated schema size, 32B
   * </ol>
   */
  @Override
  public int estimateSize() {
    return 8 + 8 + 8 + 1 + 32 + (alias == null ? 0 : ALIAS_BASE_SIZE + alias.length());
  }
}
