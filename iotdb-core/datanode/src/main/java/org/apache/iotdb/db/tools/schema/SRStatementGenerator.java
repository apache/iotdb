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

import org.apache.iotdb.commons.path.MeasurementPath;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.pipe.config.PipeConfig;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.common.AbstractDatabaseMNode;
import org.apache.iotdb.commons.schema.node.common.AbstractMeasurementMNode;
import org.apache.iotdb.commons.schema.node.role.IDeviceMNode;
import org.apache.iotdb.commons.schema.node.utils.IMNodeContainer;
import org.apache.iotdb.commons.schema.node.visitor.MNodeVisitor;
import org.apache.iotdb.commons.schema.view.LogicalViewSchema;
import org.apache.iotdb.db.queryengine.plan.relational.sql.ast.CreateOrUpdateDevice;
import org.apache.iotdb.db.queryengine.plan.statement.Statement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.AlterTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateAlignedTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.CreateTimeSeriesStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.template.ActivateTemplateStatement;
import org.apache.iotdb.db.queryengine.plan.statement.metadata.view.CreateLogicalViewStatement;
import org.apache.iotdb.db.schemaengine.SchemaEngine;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaRegionStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.attribute.DeviceAttributeStore;
import org.apache.iotdb.db.schemaengine.schemaregion.attribute.IDeviceAttributeStore;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.IMemMNode;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.mnode.info.TableDeviceInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.mtree.impl.mem.snapshot.MemMTreeSnapshotUtil;

import org.apache.tsfile.utils.Binary;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.ReadWriteIOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.StandardOpenOption;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.apache.iotdb.commons.schema.SchemaConstant.DATABASE_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.ENTITY_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.INTERNAL_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.LOGICAL_VIEW_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.MEASUREMENT_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.STORAGE_GROUP_ENTITY_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.TABLE_MNODE_TYPE;
import static org.apache.iotdb.commons.schema.SchemaConstant.isStorageGroupType;
import static org.apache.iotdb.db.schemaengine.schemaregion.tag.TagLogFile.parseByteBuffer;

public class SRStatementGenerator implements Iterator<Object>, Iterable<Object> {

  private static final Logger LOGGER = LoggerFactory.getLogger(SRStatementGenerator.class);
  private IMemMNode curNode;

  // Iterable<> cannot throw exception, so you should check lastExcept to make sure
  // that the parsing process is error-free.
  private Exception lastExcept = null;

  // Input file stream: mTree file and tag file
  private final InputStream inputStream;

  private final FileChannel tagFileChannel;

  // Mem-control
  // Note that this statistics does not have metric currently, thus it's OK to have potentially
  // multiple "-1"s. Must change this when this is taken into metrics
  private final MemSchemaRegionStatistics schemaRegionStatistics =
      new MemSchemaRegionStatistics(-1, SchemaEngine.getInstance().getSchemaEngineStatistics());
  private final IDeviceAttributeStore deviceAttributeStore =
      new DeviceAttributeStore(schemaRegionStatistics);

  // Help to record the state of traversing
  private final Deque<IMemMNode> ancestors = new ArrayDeque<>();
  private final Deque<Integer> restChildrenNum = new ArrayDeque<>();
  private final PartialPath databaseFullPath;

  // Iterable statements

  private final Deque<Object> statements = new ArrayDeque<>();

  // Utils

  private final MNodeTranslator translator = new MNodeTranslator();

  private final MemMTreeSnapshotUtil.MNodeDeserializer deserializer =
      new MemMTreeSnapshotUtil.MNodeDeserializer();

  private int nodeCount = 0;

  // Table device batch
  // We construct inner batch for better memory utilization and because there's no need to implement
  // a batch visitor for createOrUpdateDevice alone outside
  private static final int MAX_SCHEMA_BATCH_SIZE =
      PipeConfig.getInstance().getPipeSnapshotExecutionMaxBatchSize();
  private String tableName;
  private List<Object[]> tableDeviceIdList = new ArrayList<>();
  private List<String> attributeNameList = null;
  private List<Object[]> attributeValueList = new ArrayList<>();
  private boolean isClosed = false;

  public SRStatementGenerator(
      final File mtreeFile,
      final File tagFile,
      final File attributeFile,
      final PartialPath databaseFullPath)
      throws IOException {

    inputStream = Files.newInputStream(mtreeFile.toPath());

    if (tagFile != null) {
      tagFileChannel = FileChannel.open(tagFile.toPath(), StandardOpenOption.READ);
    } else {
      tagFileChannel = null;
    }

    if (Objects.nonNull(attributeFile)) {
      deviceAttributeStore.loadFromSnapshot(attributeFile.getParentFile());
    }

    this.databaseFullPath = databaseFullPath;

    Byte version = ReadWriteIOUtils.readByte(inputStream);
    curNode =
        deserializeMNode(
            ancestors, restChildrenNum, deserializer, inputStream, schemaRegionStatistics);
    nodeCount++;
  }

  @Nonnull
  @Override
  public Iterator<Object> iterator() {
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
          if (Objects.nonNull(stmt)) {
            statements.push(stmt);
          }
        }
        cleanMTreeNode(node);
        if (ancestors.isEmpty()) {
          emitDevice();
        }
        if (!statements.isEmpty()) {
          return true;
        }
      } else {
        restChildrenNum.push(childNum - 1);
        try {
          curNode =
              deserializeMNode(
                  ancestors, restChildrenNum, deserializer, inputStream, schemaRegionStatistics);
          nodeCount++;
        } catch (final IOException ioe) {
          lastExcept = ioe;
          try {
            inputStream.close();
            if (Objects.nonNull(tagFileChannel)) {
              tagFileChannel.close();
            }
            deviceAttributeStore.clear();
          } catch (final IOException e) {
            lastExcept = e;
          } finally {
            schemaRegionStatistics.clear();
          }
          return false;
        }
        final List<Object> stmts =
            curNode.accept(
                translator,
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

    if (!isClosed) {
      try {
        inputStream.close();
        if (tagFileChannel != null) {
          tagFileChannel.close();
        }
        deviceAttributeStore.clear();
      } catch (final IOException e) {
        lastExcept = e;
      } finally {
        schemaRegionStatistics.clear();
      }
      isClosed = true;
    }
    return false;
  }

  @Override
  public Object next() {
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

  private void cleanMTreeNode(final IMNode node) {
    final IMNodeContainer<IMemMNode> children = node.getAsInternalMNode().getChildren();
    nodeCount = nodeCount - children.size();
    children.values().forEach(child -> schemaRegionStatistics.releaseMemory(child.estimateSize()));
    node.getChildren().clear();
  }

  private IMemMNode deserializeMNode(
      final Deque<IMemMNode> ancestors,
      final Deque<Integer> restChildrenNum,
      final MemMTreeSnapshotUtil.MNodeDeserializer deserializer,
      final InputStream inputStream,
      final MemSchemaRegionStatistics regionStatistics)
      throws IOException {
    final byte type = ReadWriteIOUtils.readByte(inputStream);
    final int childrenNum;
    final IMemMNode node;
    switch (type) {
      case INTERNAL_MNODE_TYPE:
        childrenNum = ReadWriteIOUtils.readInt(inputStream);
        node = deserializer.deserializeInternalMNode(inputStream);
        if (ancestors.size() == 1) {
          emitDevice();
          this.tableName = node.getName();
        }
        break;
      case DATABASE_MNODE_TYPE:
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
      case TABLE_MNODE_TYPE:
        childrenNum = ReadWriteIOUtils.readInt(inputStream);
        node = deserializer.deserializeTableDeviceMNode(inputStream);
        if (ancestors.size() == 1) {
          emitDevice();
          this.tableName = node.getName();
        }
        break;
      default:
        throw new IOException("Unrecognized MNode type" + type);
    }

    regionStatistics.requestMemory(node.estimateSize());

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

  private void emitDevice() {
    if (Objects.nonNull(attributeNameList)) {
      statements.add(
          new CreateOrUpdateDevice(
              databaseFullPath.getNodes()[1],
              this.tableName,
              tableDeviceIdList,
              attributeNameList,
              attributeValueList));
      this.tableDeviceIdList = new ArrayList<>();
      this.attributeNameList = null;
      this.attributeValueList = new ArrayList<>();
    }
  }

  private class MNodeTranslator extends MNodeVisitor<List<Object>, PartialPath> {

    @Override
    public List<Object> visitBasicMNode(final IMNode<?> node, final PartialPath path) {
      if (node.isDevice()) {
        // Aligned timeSeries will be created when node pop.
        return genActivateTemplateOrUpdateDeviceStatement(node, path);
      }
      return null;
    }

    @Override
    public List<Object> visitDatabaseMNode(
        final AbstractDatabaseMNode<?, ? extends IMNode<?>> node, final PartialPath path) {
      if (node.isDevice()) {
        return genActivateTemplateOrUpdateDeviceStatement(node, path);
      }
      return null;
    }

    @Override
    public List<Object> visitMeasurementMNode(
        final AbstractMeasurementMNode<?, ? extends IMNode<?>> node, PartialPath path) {
      path = new MeasurementPath(path.getNodes());
      if (node.isLogicalView()) {
        final List<Object> statementList = new ArrayList<>();
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
          alterTimeSeriesStatement.setPath((MeasurementPath) path);
          try {
            final Pair<Map<String, String>, Map<String, String>> tagsAndAttribute =
                getTagsAndAttributes(node.getOffset());
            if (tagsAndAttribute != null) {
              alterTimeSeriesStatement.setTagsMap(tagsAndAttribute.left);
              alterTimeSeriesStatement.setAttributesMap(tagsAndAttribute.right);
              statementList.add(alterTimeSeriesStatement);
            }
          } catch (final IOException ioException) {
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
        stmt.setPath((MeasurementPath) path);
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
          } catch (final IOException ioException) {
            lastExcept = ioException;
            LOGGER.warn("Error when parser tag and attributes files", ioException);
          }
          node.setOffset(0);
        }
        return Collections.singletonList(stmt);
      }
    }

    private List<Object> genActivateTemplateOrUpdateDeviceStatement(
        final IMNode<?> node, final PartialPath path) {
      final IDeviceMNode<?> deviceMNode = node.getAsDeviceMNode();
      if (deviceMNode.isUseTemplate()) {
        return Collections.singletonList(new ActivateTemplateStatement(path));
      } else if (deviceMNode.getDeviceInfo() instanceof TableDeviceInfo
          && ((TableDeviceInfo<?>) deviceMNode.getDeviceInfo()).getAttributePointer() > -1) {
        final Map<String, Binary> tableAttributes =
            deviceAttributeStore.getAttributes(
                ((TableDeviceInfo<?>) deviceMNode.getDeviceInfo()).getAttributePointer());
        if (tableAttributes.isEmpty()) {
          return null;
        }
        if (Objects.isNull(attributeNameList)) {
          attributeNameList = new ArrayList<>(tableAttributes.keySet());
        }
        final List<Object> attributeValues =
            attributeNameList.stream().map(tableAttributes::remove).collect(Collectors.toList());
        tableAttributes.forEach(
            (attributeKey, attributeValue) -> {
              attributeNameList.add(attributeKey);
              attributeValues.add(attributeValue);
            });
        attributeValueList.add(attributeValues.toArray());
        tableDeviceIdList.add(Arrays.copyOfRange(path.getNodes(), 3, path.getNodeLength()));
        if (tableDeviceIdList.size() >= MAX_SCHEMA_BATCH_SIZE) {
          emitDevice();
        }
      }
      return null;
    }
  }

  private Statement genAlignedTimeseriesStatement(final IMNode node, final PartialPath path) {
    final IMNodeContainer<IMemMNode> measurements = node.getAsInternalMNode().getChildren();
    if (node.getAsDeviceMNode().isAligned()) {
      final CreateAlignedTimeSeriesStatement stmt = new CreateAlignedTimeSeriesStatement();
      stmt.setDevicePath(path);
      boolean hasMeasurement = false;
      for (final IMemMNode measurement : measurements.values()) {
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
            final Pair<Map<String, String>, Map<String, String>> tagsAndAttributes =
                getTagsAndAttributes(measurement.getAsMeasurementMNode().getOffset());
            if (tagsAndAttributes != null) {
              stmt.addAttributesList(tagsAndAttributes.right);
              stmt.addTagsList(tagsAndAttributes.left);
            }
          } catch (final IOException ioException) {
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

  private Pair<Map<String, String>, Map<String, String>> getTagsAndAttributes(final long offset)
      throws IOException {
    if (tagFileChannel != null) {
      final ByteBuffer byteBuffer = parseByteBuffer(tagFileChannel, offset);
      final Pair<Map<String, String>, Map<String, String>> tagsAndAttributes =
          new Pair<>(ReadWriteIOUtils.readMap(byteBuffer), ReadWriteIOUtils.readMap(byteBuffer));
      return tagsAndAttributes;
    } else {
      LOGGER.warn("Measurement has set attributes or tags, but not find snapshot files");
    }
    return null;
  }
}
