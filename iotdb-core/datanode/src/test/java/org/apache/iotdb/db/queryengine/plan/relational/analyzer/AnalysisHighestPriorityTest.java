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

package org.apache.iotdb.db.queryengine.plan.relational.analyzer;

import org.apache.iotdb.commons.queryengine.plan.relational.metadata.QualifiedObjectName;
import org.apache.iotdb.commons.schema.table.InformationSchema;

import org.junit.Test;

import java.util.Collections;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/** Queries reading only information_schema run with the highest priority (memory fallback). */
public class AnalysisHighestPriorityTest {

  private static final QualifiedObjectName TABLES =
      new QualifiedObjectName(InformationSchema.INFORMATION_DATABASE, InformationSchema.TABLES);
  private static final QualifiedObjectName COLUMNS =
      new QualifiedObjectName(InformationSchema.INFORMATION_DATABASE, InformationSchema.COLUMNS);
  private static final QualifiedObjectName QUERIES =
      new QualifiedObjectName(InformationSchema.INFORMATION_DATABASE, InformationSchema.QUERIES);

  private static Analysis analysis() {
    return new Analysis(null, Collections.emptyMap());
  }

  @Test
  public void noTablesKeepsNormalPriority() {
    assertFalse(analysis().needSetHighestPriority());
  }

  @Test
  public void onlyInformationSchemaTablesGetTheHighestPriority() {
    Analysis analysis = analysis();
    analysis.updateNeedSetHighestPriority(TABLES);
    analysis.updateNeedSetHighestPriority(COLUMNS);
    assertTrue(analysis.needSetHighestPriority());
  }

  @Test
  public void anyOtherTableKeepsNormalPriority() {
    Analysis analysis = analysis();
    analysis.updateNeedSetHighestPriority(TABLES);
    analysis.markNonInformationSchemaTableRead();
    assertFalse(analysis.needSetHighestPriority());
  }

  @Test
  public void otherTableFirstKeepsNormalPriorityToo() {
    Analysis analysis = analysis();
    analysis.markNonInformationSchemaTableRead();
    analysis.updateNeedSetHighestPriority(COLUMNS);
    assertFalse(analysis.needSetHighestPriority());
  }

  @Test
  public void queriesTableKeepsItsUpstreamRuleEvenWhenMixed() {
    Analysis analysis = analysis();
    analysis.markNonInformationSchemaTableRead();
    analysis.updateNeedSetHighestPriority(QUERIES);
    assertTrue(analysis.needSetHighestPriority());
  }
}
