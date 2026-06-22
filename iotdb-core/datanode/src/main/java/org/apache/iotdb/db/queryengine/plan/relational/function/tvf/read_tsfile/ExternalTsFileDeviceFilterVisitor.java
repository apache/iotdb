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

package org.apache.iotdb.db.queryengine.plan.relational.function.tvf.read_tsfile;

import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterVisitor;
import org.apache.iotdb.commons.schema.filter.impl.StringValueFilterVisitor;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.AttributeFilter;
import org.apache.iotdb.commons.schema.filter.impl.singlechild.TagFilter;
import org.apache.iotdb.db.i18n.DataNodeQueryMessages;

import org.apache.tsfile.file.metadata.IDeviceID;

public class ExternalTsFileDeviceFilterVisitor extends SchemaFilterVisitor<IDeviceID> {

  @Override
  protected Boolean visitNode(final SchemaFilter filter, final IDeviceID deviceID) {
    throw new UnsupportedOperationException(
        String.format(
            DataNodeQueryMessages.SCHEMA_FILTER_TYPE_IS_NOT_SUPPORTED,
            filter.getSchemaFilterType()));
  }

  @Override
  public Boolean visitTagFilter(final TagFilter filter, final IDeviceID deviceID) {
    final int index = filter.getIndex() + 1;
    final String value = index < deviceID.segmentNum() ? (String) deviceID.segment(index) : null;
    return filter.getChild().accept(StringValueFilterVisitor.getInstance(), value);
  }

  @Override
  public Boolean visitAttributeFilter(final AttributeFilter filter, final IDeviceID deviceID) {
    throw new UnsupportedOperationException(
        DataNodeQueryMessages
            .ATTRIBUTE_FILTER_IS_NOT_SUPPORTED_FOR_EXTERNAL_TSFILE_DEVICE_FILTERING);
  }
}
