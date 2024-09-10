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

package org.apache.iotdb.db.queryengine.plan.relational.planner;

import org.apache.iotdb.db.queryengine.plan.relational.metadata.Metadata;
import org.apache.iotdb.db.queryengine.plan.relational.type.TypeManager;

/**
 * A carrier of core, global, non-derived services for planner and analyzer. This is used to ease
 * the addition of new services in the future without having to modify large portions the planner
 * and analyzer just to pass around the service.
 */
public class PlannerContext {

  private final Metadata metadata;

  private final TypeManager typeManager;

  public PlannerContext(Metadata metadata, TypeManager typeManager) {
    this.metadata = metadata;
    this.typeManager = typeManager;
  }

  public Metadata getMetadata() {
    return metadata;
  }

  public TypeManager getTypeManager() {
    return typeManager;
  }
}
