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
package org.apache.iotdb.db.metadata.upgrade;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.LocalSchemaProcessor;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.logfile.MLogReader;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.IStorageGroupMNode;
import org.apache.iotdb.db.metadata.mnode.InternalMNode;
import org.apache.iotdb.db.metadata.mnode.MNodeUtils;
import org.apache.iotdb.db.metadata.mnode.MeasurementMNode;
import org.apache.iotdb.db.metadata.mnode.StorageGroupMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.metadata.tag.TagLogFile;
import org.apache.iotdb.db.qp.physical.PhysicalPlan;
import org.apache.iotdb.db.qp.physical.sys.ChangeTagOffsetPlan;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.MNodePlan;
import org.apache.iotdb.db.qp.physical.sys.MeasurementMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.qp.physical.sys.SetTemplatePlan;
import org.apache.iotdb.db.qp.physical.sys.StorageGroupMNodePlan;
import org.apache.iotdb.db.qp.physical.sys.UnsetTemplatePlan;
import org.apache.iotdb.db.service.IoTDB;
import org.apache.iotdb.tsfile.utils.Pair;
import org.apache.iotdb.tsfile.write.schema.IMeasurementSchema;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_ROOT;
import static org.apache.iotdb.commons.conf.IoTDBConstant.PATH_SEPARATOR;

/**
 * IoTDB after v0.13 only support upgrade from v0.12x. This class implements the upgrade program.
 */
public class MetadataUpgrader {

  private static final Logger logger = LoggerFactory.getLogger(MetadataUpgrader.class);

  private IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();
  private String schemaDirPath = config.getSchemaDir();

  private String mlogFilePath = schemaDirPath + File.separator + MetadataConstant.METADATA_LOG;
  private File mlogFile = new File(mlogFilePath);

  private String tagFilePath = schemaDirPath + File.separator + MetadataConstant.TAG_LOG;
  private File tagFile = new File(tagFilePath);

  private String mtreeSnapshotPath =
      schemaDirPath + File.separator + MetadataConstant.MTREE_SNAPSHOT;
  private String mtreeSnapshotTmpPath =
      schemaDirPath + File.separator + MetadataConstant.MTREE_SNAPSHOT_TMP;
  private File snapshotFile = new File(mtreeSnapshotPath);
  private File snapshotTmpFile = new File(mtreeSnapshotTmpPath);

  LocalSchemaProcessor schemaProcessor = IoTDB.schemaProcessor;

  /**
   * There are at most four files of old versions:
   *
   * <ol>
   *   <li>snapshot: store the base MTree
   *   <li>mlog: store the metadata operations based on the exising metadata stored in snapshot
   *   <li>tag file: store the tag info of timeseries and snapshot and mlog both may hold offsets
   *       pointing to some part of this file
   *   <li>tmp snapshot: a tmp file generated during the process of creating snapshot; this file is
   *       useless
   * </ol>
   *
   * <p>The purpose of upgrader is to recover metadata from the existing files and split and store
   * them into files in certain storage group dirs. The upgrader will execute the following steps in
   * order:
   *
   * <ol>
   *   <li>Deserialize the snapshot and recover the MRTree into memory
   *   <li>Try set storage group based on the recovered StorageGroupMNodes and create timeseries
   *       based on the recovered MeasurementMNode
   *   <li>Redo the mlog
   *   <li>delete the files of version, the order is:
   *       <ol>
   *         <li>mlog
   *         <li>snapshot
   *         <li>tag file
   *         <li>tmp snapshot
   *       </ol>
   * </ol>
   */
  public static synchronized void upgrade() throws IOException {
    MetadataUpgrader upgrader = new MetadataUpgrader();
    logger.info("Start upgrading metadata files.");
    if (upgrader.clearEnvBeforeUpgrade()) {
      logger.info("Metadata files have already been upgraded.");
      return;
    }
    IoTDB.configManager.init();
    try {
      upgrader.reloadMetadataFromSnapshot();
      upgrader.redoMLog();
      upgrader.clearOldFiles();
      logger.info("Finish upgrading metadata files.");
    } finally {
      IoTDB.configManager.clear();
    }
  }

  private MetadataUpgrader() {}

  public boolean clearEnvBeforeUpgrade() throws IOException {
    if (mlogFile.exists()) {
      // the existence of old mlog means the upgrade was interrupted, thus clear the tmp results.
      File dir = new File(schemaDirPath);
      File[] sgDirs = dir.listFiles((dir1, name) -> name.startsWith(PATH_ROOT + PATH_SEPARATOR));
      if (sgDirs == null) {
        return false;
      }
      for (File sgDir : sgDirs) {
        File[] sgFiles = sgDir.listFiles();
        if (sgFiles == null) {
          continue;
        }
        for (File sgFile : sgFiles) {
          if (!sgFile.delete()) {
            String errorMessage =
                String.format(
                    "Cannot delete file %s in dir %s during metadata upgrade",
                    sgFile.getName(), sgDir.getName());
            logger.error(errorMessage);
            throw new IOException(errorMessage);
          }
        }
      }
      return false;
    } else {
      deleteFile(tagFile);
      deleteFile(snapshotTmpFile);
      deleteFile(snapshotFile);
      deleteFile(mlogFile);
      return true;
    }
  }

  public void clearOldFiles() throws IOException {
    deleteFile(mlogFile);
    deleteFile(snapshotFile);
    deleteFile(tagFile);
    deleteFile(snapshotTmpFile);
  }

  private void deleteFile(File file) throws IOException {
    if (!file.exists()) {
      return;
    }

    if (!file.delete()) {
      String errorMessage =
          String.format("Cannot delete file %s during metadata upgrade", file.getName());
      logger.error(errorMessage);
      throw new IOException(errorMessage);
    }
  }

  public void reloadMetadataFromSnapshot() throws IOException {
    if (!snapshotFile.exists()) {
      return;
    }
    Map<IStorageGroupMNode, List<IMeasurementMNode>> sgMeasurementMap =
        deserializeFrom(snapshotFile);
    IMeasurementSchema schema;
    CreateTimeSeriesPlan createTimeSeriesPlan;
    Pair<Map<String, String>, Map<String, String>> tagAttributePair;
    Map<String, String> tags = null;
    Map<String, String> attributes = null;
    try (TagLogFile tagLogFile = new TagLogFile(schemaDirPath, MetadataConstant.TAG_LOG)) {
      for (IStorageGroupMNode storageGroupMNode : sgMeasurementMap.keySet()) {
        try {
          schemaProcessor.setStorageGroup(storageGroupMNode.getPartialPath());
          for (IMeasurementMNode measurementMNode : sgMeasurementMap.get(storageGroupMNode)) {
            schema = measurementMNode.getSchema();
            if (measurementMNode.getOffset() != -1) {
              tagAttributePair =
                  tagLogFile.read(config.getTagAttributeTotalSize(), measurementMNode.getOffset());
              if (tagAttributePair != null) {
                tags = tagAttributePair.left;
                attributes = tagAttributePair.right;
              }
            }
            createTimeSeriesPlan =
                new CreateTimeSeriesPlan(
                    measurementMNode.getPartialPath(),
                    schema.getType(),
                    schema.getEncodingType(),
                    schema.getCompressor(),
                    schema.getProps(),
                    tags,
                    attributes,
                    measurementMNode.getAlias());
            schemaProcessor.createTimeseries(createTimeSeriesPlan);
          }
        } catch (MetadataException e) {
          logger.error("Error occurred during recovering metadata from snapshot", e);
          e.printStackTrace();
          throw new IOException(e);
        }
      }
    }
  }

  private Map<IStorageGroupMNode, List<IMeasurementMNode>> deserializeFrom(File mtreeSnapshot)
      throws IOException {
    try (MLogReader mLogReader = new MLogReader(mtreeSnapshot)) {
      Map<IStorageGroupMNode, List<IMeasurementMNode>> sgMeasurementMap = new HashMap<>();
      deserializeFromReader(mLogReader, sgMeasurementMap);
      return sgMeasurementMap;
    }
  }

  private void deserializeFromReader(
      MLogReader mLogReader, Map<IStorageGroupMNode, List<IMeasurementMNode>> sgMeasurementMap)
      throws IOException {
    Deque<IMNode> nodeStack = new ArrayDeque<>();
    IMNode node = null;
    List<IMeasurementMNode> measurementMNodeList = new LinkedList<>();
    while (mLogReader.hasNext()) {
      PhysicalPlan plan = mLogReader.next();
      if (plan == null) {
        continue;
      }
      int childrenSize = 0;
      if (plan instanceof StorageGroupMNodePlan) {
        node = StorageGroupMNode.deserializeFrom((StorageGroupMNodePlan) plan);
        childrenSize = ((StorageGroupMNodePlan) plan).getChildSize();
        sgMeasurementMap.put(node.getAsStorageGroupMNode(), measurementMNodeList);
        measurementMNodeList = new LinkedList<>();
      } else if (plan instanceof MeasurementMNodePlan) {
        node = MeasurementMNode.deserializeFrom((MeasurementMNodePlan) plan);
        childrenSize = ((MeasurementMNodePlan) plan).getChildSize();
        measurementMNodeList.add(node.getAsMeasurementMNode());
      } else if (plan instanceof MNodePlan) {
        node = InternalMNode.deserializeFrom((MNodePlan) plan);
        childrenSize = ((MNodePlan) plan).getChildSize();
      }

      if (childrenSize != 0) {
        ConcurrentHashMap<String, IMNode> childrenMap = new ConcurrentHashMap<>();
        for (int i = 0; i < childrenSize; i++) {
          IMNode child = nodeStack.removeFirst();
          childrenMap.put(child.getName(), child);
          if (child.isMeasurement()) {
            if (!node.isEntity()) {
              node = MNodeUtils.setToEntity(node);
            }
            String alias = child.getAsMeasurementMNode().getAlias();
            if (alias != null) {
              node.getAsEntityMNode().addAlias(alias, child.getAsMeasurementMNode());
            }
          }
          child.setParent(node);
        }
        node.setChildren(childrenMap);
      }
      nodeStack.push(node);
    }
    if (node == null || !PATH_ROOT.equals(node.getName())) {
      logger.error("Snapshot file corrupted!");
      throw new IOException("Snapshot file corrupted!");
    }
  }

  public void redoMLog() throws IOException {
    PhysicalPlan plan;
    // templateName -> path -> plan
    Map<String, Map<String, SetTemplatePlan>> setTemplatePlanAboveSG = new HashMap<>();
    try (MLogReader mLogReader = new MLogReader(mlogFilePath);
        TagLogFile tagLogFile = new TagLogFile(schemaDirPath, MetadataConstant.TAG_LOG)) {
      while (mLogReader.hasNext()) {
        plan = mLogReader.next();
        try {
          switch (plan.getOperatorType()) {
            case CREATE_TIMESERIES:
              processCreateTimeseries((CreateTimeSeriesPlan) plan, schemaProcessor, tagLogFile);
              break;
            case CHANGE_TAG_OFFSET:
              processChangeTagOffset((ChangeTagOffsetPlan) plan, schemaProcessor, tagLogFile);
              break;
            case SET_STORAGE_GROUP:
              processSetStorageGroup(
                  (SetStorageGroupPlan) plan, schemaProcessor, setTemplatePlanAboveSG);
              break;
            case SET_TEMPLATE:
              processSetTemplate((SetTemplatePlan) plan, schemaProcessor, setTemplatePlanAboveSG);
              break;
            case UNSET_TEMPLATE:
              processUnSetTemplate(
                  (UnsetTemplatePlan) plan, schemaProcessor, setTemplatePlanAboveSG);
              break;
            default:
              schemaProcessor.operation(plan);
          }
        } catch (MetadataException e) {
          logger.error("Error occurred during redo mlog: ", e);
          e.printStackTrace();
          throw new IOException(e);
        }
      }
    }
  }

  private void processCreateTimeseries(
      CreateTimeSeriesPlan createTimeSeriesPlan,
      LocalSchemaProcessor schemaProcessor,
      TagLogFile tagLogFile)
      throws MetadataException, IOException {
    long offset;
    offset = createTimeSeriesPlan.getTagOffset();
    createTimeSeriesPlan.setTagOffset(-1);
    createTimeSeriesPlan.setTags(null);
    createTimeSeriesPlan.setAttributes(null);
    schemaProcessor.operation(createTimeSeriesPlan);
    if (offset != -1) {
      rewriteTagAndAttribute(createTimeSeriesPlan.getPath(), offset, schemaProcessor, tagLogFile);
    }
  }

  private void processChangeTagOffset(
      ChangeTagOffsetPlan changeTagOffsetPlan,
      LocalSchemaProcessor schemaProcessor,
      TagLogFile tagLogFile)
      throws MetadataException, IOException {
    rewriteTagAndAttribute(
        changeTagOffsetPlan.getPath(),
        changeTagOffsetPlan.getOffset(),
        schemaProcessor,
        tagLogFile);
  }

  private void rewriteTagAndAttribute(
      PartialPath path, long offset, LocalSchemaProcessor schemaProcessor, TagLogFile tagLogFile)
      throws IOException, MetadataException {
    Pair<Map<String, String>, Map<String, String>> pair =
        tagLogFile.read(config.getTagAttributeTotalSize(), offset);
    schemaProcessor.addTags(pair.left, path);
    schemaProcessor.addAttributes(pair.right, path);
  }

  private void processSetStorageGroup(
      SetStorageGroupPlan setStorageGroupPlan,
      LocalSchemaProcessor schemaProcessor,
      Map<String, Map<String, SetTemplatePlan>> setTemplatePlanAboveSG)
      throws IOException, MetadataException {
    schemaProcessor.operation(setStorageGroupPlan);
    String storageGroupPath = setStorageGroupPlan.getPath().getFullPath();
    String templatePath;
    for (Map<String, SetTemplatePlan> pathPlanMap : setTemplatePlanAboveSG.values()) {
      for (SetTemplatePlan setTemplatePlan : pathPlanMap.values()) {
        templatePath = setTemplatePlan.getPrefixPath();
        if (storageGroupPath.startsWith(templatePath)) {
          schemaProcessor.setSchemaTemplate(
              new SetTemplatePlan(setTemplatePlan.getTemplateName(), storageGroupPath));
        }
      }
    }
  }

  private void processSetTemplate(
      SetTemplatePlan setTemplatePlan,
      LocalSchemaProcessor schemaProcessor,
      Map<String, Map<String, SetTemplatePlan>> setTemplatePlanAboveSG)
      throws MetadataException {
    PartialPath path = new PartialPath(setTemplatePlan.getPrefixPath());
    List<PartialPath> storageGroupPathList = schemaProcessor.getMatchedStorageGroups(path, true);
    if (storageGroupPathList.size() > 1 || !path.equals(storageGroupPathList.get(0))) {
      String templateName = setTemplatePlan.getTemplateName();
      if (!setTemplatePlanAboveSG.containsKey(templateName)) {
        setTemplatePlanAboveSG.put(templateName, new HashMap<>());
      }
      setTemplatePlanAboveSG
          .get(templateName)
          .put(setTemplatePlan.getPrefixPath(), setTemplatePlan);
    }

    for (PartialPath storageGroupPath : storageGroupPathList) {
      schemaProcessor.setSchemaTemplate(
          new SetTemplatePlan(setTemplatePlan.getTemplateName(), storageGroupPath.getFullPath()));
    }
  }

  private void processUnSetTemplate(
      UnsetTemplatePlan unsetTemplatePlan,
      LocalSchemaProcessor schemaProcessor,
      Map<String, Map<String, SetTemplatePlan>> setTemplatePlanAboveSG)
      throws MetadataException {
    PartialPath path = new PartialPath(unsetTemplatePlan.getPrefixPath());
    List<PartialPath> storageGroupPathList = schemaProcessor.getMatchedStorageGroups(path, true);
    if (storageGroupPathList.size() > 1 || !path.equals(storageGroupPathList.get(0))) {
      setTemplatePlanAboveSG
          .get(unsetTemplatePlan.getTemplateName())
          .remove(unsetTemplatePlan.getPrefixPath());
    }
    for (PartialPath storageGroupPath : storageGroupPathList) {
      schemaProcessor.unsetSchemaTemplate(
          new UnsetTemplatePlan(
              storageGroupPath.getFullPath(), unsetTemplatePlan.getTemplateName()));
    }
  }
}
