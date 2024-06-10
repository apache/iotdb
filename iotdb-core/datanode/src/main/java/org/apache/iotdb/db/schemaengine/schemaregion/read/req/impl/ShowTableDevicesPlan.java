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
 *
 */

package org.apache.iotdb.db.schemaengine.schemaregion.read.req.impl;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.impl.AndFilter;

import java.util.List;

public class ShowTableDevicesPlan {

  private PartialPath devicePattern;

  private SchemaFilter attributeFilter;

  public ShowTableDevicesPlan(PartialPath devicePattern, SchemaFilter attributeFilter) {
    this.devicePattern = devicePattern;
    this.attributeFilter = attributeFilter;
  }

  private SchemaFilter getAttributeFilter(List<SchemaFilter> filterList) {
    if (filterList.isEmpty()) {
      return null;
    }
    AndFilter andFilter;
    SchemaFilter latestFilter = filterList.get(0);
    for (int i = 1; i < filterList.size(); i++) {
      andFilter = new AndFilter(latestFilter, filterList.get(i));
      latestFilter = andFilter;
    }
    return latestFilter;
  }

  public PartialPath getDevicePattern() {
    return devicePattern;
  }

  public SchemaFilter getAttributeFilter() {
    return attributeFilter;
  }
}
