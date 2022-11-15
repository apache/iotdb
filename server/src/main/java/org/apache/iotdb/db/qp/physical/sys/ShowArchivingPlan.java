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
package org.apache.iotdb.db.qp.physical.sys;

import org.apache.iotdb.db.metadata.path.PartialPath;

import java.util.List;

public class ShowArchivingPlan extends ShowPlan {
  private final List<PartialPath> storageGroups;

  private final boolean showAll;

  public ShowArchivingPlan(List<PartialPath> storageGroups, boolean showAll) {
    super(ShowContentType.SHOW_ARCHIVING);
    this.storageGroups = storageGroups;
    this.showAll = showAll;
  }

  @Override
  public List<PartialPath> getPaths() {
    return null;
  }

  public List<PartialPath> getStorageGroups() {
    return storageGroups;
  }

  public boolean isShowAll() {
    return showAll;
  }
}
