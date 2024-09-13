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

package org.apache.iotdb.db.tools.schema;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.common.AbstractDatabaseMNode;
import org.apache.iotdb.commons.schema.node.common.AbstractMeasurementMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.commons.schema.node.visitor.MNodeVisitor;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.CreateLogicalViewStatement;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.snapshot.MemMTreeSnapshotUtil;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import static org.apache.iotdb.commons.schema.SchemaConstant.ENTITY_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.INTERNAL_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.LOGICAL_VIEW_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.MEASUREMENT_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.STORAGE_GROUP_ENTITY_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.STORAGE_GROUP_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.isStorageGroupType;
import static org.apache.iotdb.db.schemaengine.schemaregion.tag.TagLogFile.parseByteBuffer;

public class SRStatementGenerator implements Iterator<Statement>, Iterable<Statement> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SRStatementGenerator.class);
  private IMemMNode curNode;

  // Iterable<> cannot throw exception, so you should check lastExcept to make sure
  // that the parsing process is error-free.
  private Exception lastExcept = null;

  // Input file stream: mtree file and tag file
  private final InputStream inputStream;

  private final FileChannel tagFileChannel;

  // Help to record the state of traversing
  private final Deque<IMemMNode> ancestors = new ArrayDeque<>();
  private final Deque<Integer> restChildrenNum = new ArrayDeque<>();
  private final PartialPath databaseFullPath;

  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  // Iterable statements

  private final Deque<Statement> statements = new ArrayDeque<>();

  // Utils

  private final MNodeTranslater translater = new MNodeTranslater();

  private final MemMTreeSnapshotUtil.MNodeDeserializer deserializer =
      new MemMTreeSnapshotUtil.MNodeDeserializer();

  private int nodeCount = 0;

  public SRStatementGenerator(File mtreeFile, File tagFile, PartialPath databaseFullPath)
      throws IOException {

    inputStream = Files.newInputStream(mtreeFile.toPath());

    if (tagFile != null) {
      tagFileChannel = FileChannel.open(tagFile.toPath(), StandardOpenOption.READ);
    } else {
      tagFileChannel = null;
    }

    this.databaseFullPath = databaseFullPath;

    Byte version = ReadWriteIOUtils.readByte(inputStream);
    curNode = deserializeMNode(ancestors, restChildrenNum, deserializer, inputStream);
    nodeCount++;
  }

  @Override
  public Iterator<Statement> iterator() {
    return this;
  }

  @Override
  public boolean hasNext() {
    if (!statements.isEmpty()) {
      return true;
    }
    if (lastExcept != null) {
      return false;
    }
    while (!ancestors.isEmpty()) {
      final int childNum = restChildrenNum.pop();
      if (childNum == 0) {
        final IMemMNode node = ancestors.pop();
        if (node.isDevice() && node.getAsDeviceMNode().isAligned()) {
          final Statement stmt =
              genAlignedTimeseriesStatement(
                  // skip common database
                  node, databaseFullPath.concatPath(node.getPartialPath(), 1));
          statements.push(stmt);
        }
        cleanMtreeNode(node);
        if (!statements.isEmpty()) {
          return true;
        }
      } else {
        restChildrenNum.push(childNum - 1);
        try {
          curNode = deserializeMNode(ancestors, restChildrenNum, deserializer, inputStream);
          nodeCount++;
        } catch (IOException ioe) {
          lastExcept = ioe;
          try {
            inputStream.close();
            tagFileChannel.close();

          } catch (IOException e) {
            lastExcept = e;
          }
          return false;
        }
        final List<Statement> stmts =
            curNode.accept(
                translater,
                // skip common database
                databaseFullPath.concatPath(curNode.getPartialPath(), 1));
        if (stmts != null) {
          statements.addAll(stmts);
        }
        if (!statements.isEmpty()) {
          return true;
        }
      }
    }
    try {
      inputStream.close();
      if (tagFileChannel != null) {
        tagFileChannel.close();
      }
    } catch (IOException e) {
      lastExcept = e;
    }
    return false;
  }

  @Override
  public Statement next() {
    if (!hasNext()) {
      throw new NoSuchElementException();
    }
    return statements.pop();
  }

  public void checkException() throws IOException {
    if (lastExcept != null) {
      throw new IOException(lastExcept);
    }
  }

  private void cleanMtreeNode(IMNode node) {
    final IMNodeContainer<IMemMNode> children = node.getAsInternalMNode().getChildren();
    nodeCount = nodeCount - children.size();
    node.getChildren().clear();
  }

  private static IMemMNode deserializeMNode(
      Deque<IMemMNode> ancestors,
      Deque<Integer> restChildrenNum,
      MemMTreeSnapshotUtil.MNodeDeserializer deserializer,
      InputStream inputStream)
      throws IOException {
    final byte type = ReadWriteIOUtils.readByte(inputStream);
    final int childrenNum;
    final IMemMNode node;
    switch (type) {
      case INTERNAL_MNODE_TYPE:
        childrenNum = ReadWriteIOUtils.readInt(inputStream);
        node = deserializer.deserializeInternalMNode(inputStream);
        break;
      case STORAGE_GROUP_MNODE_TYPE:
        childrenNum = ReadWriteIOUtils.readInt(inputStream);
        node = deserializer.deserializeStorageGroupMNode(inputStream);
        break;
      case ENTITY_MNODE_TYPE:
        childrenNum = ReadWriteIOUtils.readInt(inputStream);
        node = deserializer.deserializeEntityMNode(inputStream);
        break;
      case STORAGE_GROUP_ENTITY_MNODE_TYPE:
        childrenNum = ReadWriteIOUtils.readInt(inputStream);
        node = deserializer.deserializeStorageGroupEntityMNode(inputStream);
        break;
      case MEASUREMENT_MNODE_TYPE:
        childrenNum = 0;
        node = deserializer.deserializeMeasurementMNode(inputStream);
        break;
      case LOGICAL_VIEW_MNODE_TYPE:
        childrenNum = 0;
        node = deserializer.deserializeLogicalViewMNode(inputStream);
        break;
      default:
        throw new IOException("Unrecognized MNode type" + type);
    }

    if (!ancestors.isEmpty()) {
      final IMemMNode parent = ancestors.peek();
      node.setParent(ancestors.peek());
      parent.addChild(node);
    }

    if (childrenNum > 0 || isStorageGroupType(type)) {
      ancestors.push(node);
      restChildrenNum.push(childrenNum);
    }
    return node;
  }

  private class MNodeTranslater extends MNodeVisitor<List<Statement>, PartialPath> {

    @Override
    public List<Statement> visitBasicMNode(IMNode<?> node, PartialPath path) {
      if (node.isDevice()) {
        // Aligned timeseries will be created when node pop.
        return SRStatementGenerator.genActivateTemplateStatement(node, path);
      }
      return null;
    }

    @Override
    public List<Statement> visitDatabaseMNode(
        AbstractDatabaseMNode<?, ? extends IMNode<?>> node, PartialPath path) {
      if (node.isDevice()) {
        return SRStatementGenerator.genActivateTemplateStatement(node, path);
      }
      return null;
    }

    @Override
    public List<Statement> visitMeasurementMNode(
        AbstractMeasurementMNode<?, ? extends IMNode<?>> node, PartialPath path) {
      if (node.isLogicalView()) {
        List<Statement> statementList = new ArrayList<>();
        final CreateLogicalViewStatement stmt = new CreateLogicalViewStatement();
        final LogicalViewSchema viewSchema =
            (LogicalViewSchema) node.getAsMeasurementMNode().getSchema();
        if (viewSchema != null) {
          stmt.setTargetFullPaths(Collections.singletonList(path));
          stmt.setViewExpressions(Collections.singletonList(viewSchema.getExpression()));
          statementList.add(stmt);
        }
        if (node.getOffset() >= 0) {
          final AlterTimeSeriesStatement alterTimeSeriesStatement =
              new AlterTimeSeriesStatement(true);
          alterTimeSeriesStatement.setAlterType(AlterTimeSeriesStatement.AlterType.UPSERT);
          alterTimeSeriesStatement.setPath(path);
          try {
            Pair<Map<String, String>, Map<String, String>> tagsAndAttribute =
                getTagsAndAttributes(node.getOffset());
            if (tagsAndAttribute != null) {
              alterTimeSeriesStatement.setTagsMap(tagsAndAttribute.left);
              alterTimeSeriesStatement.setAttributesMap(tagsAndAttribute.right);
              statementList.add(alterTimeSeriesStatement);
            }
          } catch (IOException ioException) {
            lastExcept = ioException;
            LOGGER.warn(
                "Error when parse tag and attributes file of node path {}", path, ioException);
          }
        }
        return statementList;
      } else if (node.getParent().getAsDeviceMNode().isAligned()) {
        return null;
      } else {
        final CreateTimeSeriesStatement stmt = new CreateTimeSeriesStatement();
        stmt.setPath(new MeasurementPath(path.getNodes()));
        stmt.setAlias(node.getAlias());
        stmt.setCompressor(node.getAsMeasurementMNode().getSchema().getCompressor());
        stmt.setDataType(node.getDataType());
        stmt.setEncoding(node.getAsMeasurementMNode().getSchema().getEncodingType());
        if (node.getOffset() >= 0) {
          try {
            final Pair<Map<String, String>, Map<String, String>> tagsAndAttributes =
                getTagsAndAttributes(node.getOffset());
            if (tagsAndAttributes != null) {
              stmt.setTags(tagsAndAttributes.left);
              stmt.setAttributes(tagsAndAttributes.right);
            }
          } catch (IOException ioException) {
            lastExcept = ioException;
            LOGGER.warn("Error when parser tag and attributes files", ioException);
          }
          node.setOffset(0);
        }
        return Collections.singletonList(stmt);
      }
    }
  }

  private static List<Statement> genActivateTemplateStatement(IMNode node, PartialPath path) {
    if (node.getAsDeviceMNode().isUseTemplate()) {
      return Collections.singletonList(new ActivateTemplateStatement(path));
    }
    return null;
  }

  private Statement genAlignedTimeseriesStatement(IMNode node, PartialPath path) {
    final IMNodeContainer<IMemMNode> measurements = node.getAsInternalMNode().getChildren();
    if (node.getAsDeviceMNode().isAligned()) {
      final CreateAlignedTimeSeriesStatement stmt = new CreateAlignedTimeSeriesStatement();
      stmt.setDevicePath(path);
      boolean hasMeasurement = false;
      for (IMemMNode measurement : measurements.values()) {
        if (!measurement.isMeasurement() || measurement.getAsMeasurementMNode().isLogicalView()) {
          continue;
        }
        hasMeasurement = true;
        stmt.addMeasurement(measurement.getName());
        stmt.addDataType(measurement.getAsMeasurementMNode().getDataType());
        if (measurement.getAlias() != null) {
          stmt.addAliasList(measurement.getAlias());
        } else {
          stmt.addAliasList(null);
        }
        stmt.addEncoding(measurement.getAsMeasurementMNode().getSchema().getEncodingType());
        stmt.addCompressor(measurement.getAsMeasurementMNode().getSchema().getCompressor());
        if (measurement.getAsMeasurementMNode().getOffset() >= 0) {
          try {
            Pair<Map<String, String>, Map<String, String>> tagsAndAttributes =
                getTagsAndAttributes(measurement.getAsMeasurementMNode().getOffset());
            if (tagsAndAttributes != null) {
              stmt.addAttributesList(tagsAndAttributes.right);
              stmt.addTagsList(tagsAndAttributes.left);
            }
          } catch (IOException ioException) {
            lastExcept = ioException;
            LOGGER.warn(
                "Error when parse tag and attributes file of node path {}", path, ioException);
          }
          measurement.getAsMeasurementMNode().setOffset(0);
        } else {
          stmt.addAttributesList(null);
          stmt.addTagsList(null);
        }
      }
      return hasMeasurement ? stmt : null;
    }
    return null;
  }

  private Pair<Map<String, String>, Map<String, String>> getTagsAndAttributes(long offset)
      throws IOException {
    if (tagFileChannel != null) {
      ByteBuffer byteBuffer = parseByteBuffer(tagFileChannel, offset);
      Pair<Map<String, String>, Map<String, String>> tagsAndAttributes =
          new Pair<>(ReadWriteIOUtils.readMap(byteBuffer), ReadWriteIOUtils.readMap(byteBuffer));
      return tagsAndAttributes;
    } else {
      LOGGER.warn("Measurement has set attributes or tags, but not find snapshot files");
    }
    return null;
  }
}
