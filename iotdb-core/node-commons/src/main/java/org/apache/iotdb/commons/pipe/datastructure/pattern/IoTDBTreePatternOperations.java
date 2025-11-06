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

package org.apache.iotdb.commons.pipe.datastructure.pattern;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;

import java.util.List;

/**
 * An interface for TreePattern classes that support IoTDB-specific path matching operations, such
 * as those used by schema-aware visitors.
 */
public abstract class IoTDBTreePatternOperations extends TreePattern {

  protected IoTDBTreePatternOperations(final boolean isTreeModelDataAllowedToBeCaptured) {
    super(isTreeModelDataAllowedToBeCaptured);
  }

  //////////////////////////// IoTDB Pattern Operations ////////////////////////////

  public abstract boolean matchPrefixPath(final String path);

  public abstract boolean matchDevice(final String devicePath);

  public abstract boolean matchTailNode(final String tailNode);

  public abstract List<PartialPath> getIntersection(final PartialPath partialPath);

  public abstract PathPatternTree getIntersection(final PathPatternTree patternTree);

  public abstract boolean isPrefixOrFullPath();

  public abstract boolean mayMatchMultipleTimeSeriesInOneDevice();
}
