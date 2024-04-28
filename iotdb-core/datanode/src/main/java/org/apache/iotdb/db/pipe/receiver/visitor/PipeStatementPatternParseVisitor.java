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

package org.apache.iotdb.db.pipe.receiver.visitor;

import org.apache.iotdb.commons.pipe.pattern.IoTDBPipePattern;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementNode;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;
import org.apache.iotdb.db.tools.schema.SRStatementGenerator;

import java.util.Optional;

/**
 * The {@link PipeStatementPatternParseVisitor} will transform the schema {@link Statement}s using
 * {@link IoTDBPipePattern}. Rule:
 *
 * <p>1. All patterns in the output {@link Statement} will be the intersection of the original
 * {@link Statement}'s patterns and the given {@link IoTDBPipePattern}.
 *
 * <p>2. If a pattern does not intersect with the {@link IoTDBPipePattern}, it's dropped.
 *
 * <p>3. If all the patterns in the {@link Statement} is dropped, the {@link Statement} is dropped.
 *
 * <p>4. The output {@link Statement} can be the altered form of the original one because it's read
 * from the {@link SRStatementGenerator} and will no longer be used.
 */
public class PipeStatementPatternParseVisitor
    extends StatementVisitor<Optional<Statement>, IoTDBPipePattern> {
  @Override
  public Optional<Statement> visitNode(final StatementNode node, final IoTDBPipePattern pattern) {
    return Optional.of((Statement) node);
  }
}
