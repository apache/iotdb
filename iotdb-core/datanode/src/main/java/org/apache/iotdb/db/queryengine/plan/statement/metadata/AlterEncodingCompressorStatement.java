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

package org.apache.iotdb.db.queryengine.plan.statement.metadata;

import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.path.PathPatternTree;
import org.apache.iotdb.db.queryengine.plan.analyze.QueryType;
import org.apache.iotdb.db.queryengine.plan.statement.IConfigStatement;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.StatementType;
import org.apache.iotdb.db.queryengine.plan.statement.StatementVisitor;

import org.apache.tsfile.file.metadata.enums.CompressionType;
import org.apache.tsfile.file.metadata.enums.TSEncoding;

import java.util.List;
import java.util.Objects;

public class AlterEncodingCompressorStatement extends Statement implements IConfigStatement {

  private PathPatternTree patternTree;
  private final TSEncoding encoding;
  private final CompressionType compressor;
  private final boolean ifExists;
  private final boolean ifPermitted;
  private boolean withAudit = false;

  public AlterEncodingCompressorStatement(
      final PathPatternTree pathPatternTree,
      final TSEncoding encoding,
      final CompressionType compressor,
      final boolean ifExists,
      final boolean ifPermitted) {
    statementType = StatementType.ALTER_ENCODING_COMPRESSOR;
    this.patternTree = pathPatternTree;
    this.encoding = encoding;
    this.compressor = compressor;
    this.ifExists = ifExists;
    this.ifPermitted = ifPermitted;
  }

  public TSEncoding getEncoding() {
    return encoding;
  }

  public CompressionType getCompressor() {
    return compressor;
  }

  public PathPatternTree getPatternTree() {
    return patternTree;
  }

  public void setPatternTree(final PathPatternTree patternTree) {
    this.patternTree = patternTree;
  }

  public boolean ifExists() {
    return ifExists;
  }

  public boolean ifPermitted() {
    return ifPermitted;
  }

  public void setWithAudit(final boolean withAudit) {
    this.withAudit = withAudit;
  }

  public boolean isWithAudit() {
    return withAudit;
  }

  @Override
  public List<? extends PartialPath> getPaths() {
    return patternTree.getAllPathPatterns();
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (obj == null || getClass() != obj.getClass()) {
      return false;
    }
    final AlterEncodingCompressorStatement that = (AlterEncodingCompressorStatement) obj;
    return Objects.equals(this.patternTree, that.patternTree)
        && Objects.equals(this.encoding, that.encoding)
        && Objects.equals(this.compressor, that.compressor)
        && Objects.equals(this.ifExists, that.ifExists)
        && Objects.equals(this.ifPermitted, that.ifPermitted);
  }

  @Override
  public int hashCode() {
    return Objects.hash(patternTree, encoding, compressor, ifExists, ifPermitted);
  }

  @Override
  public <R, C> R accept(final StatementVisitor<R, C> visitor, final C context) {
    return visitor.visitAlterEncodingCompressor(this, context);
  }

  @Override
  public QueryType getQueryType() {
    return QueryType.WRITE;
  }
}
