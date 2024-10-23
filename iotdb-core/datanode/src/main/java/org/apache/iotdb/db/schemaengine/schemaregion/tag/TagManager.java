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

package org.apache.iotdb.db.schemaengine.schemaregion.tag;

import org.apache.iotdb.commons.conf.CommonConfig;
import org.apache.iotdb.commons.conf.CommonDescriptor;
import org.apache.iotdb.commons.exception.MetadataException;
import org.apache.iotdb.commons.file.SystemFileFactory;
import org.apache.iotdb.commons.path.PartialPath;
import org.apache.iotdb.commons.schema.SchemaConstant;
import org.apache.iotdb.commons.schema.filter.SchemaFilter;
import org.apache.iotdb.commons.schema.filter.SchemaFilterType;
import org.apache.iotdb.commons.schema.filter.impl.TagFilter;
import org.apache.iotdb.commons.schema.node.IMNode;
import org.apache.iotdb.commons.schema.node.role.IMeasurementMNode;
import org.apache.iotdb.commons.schema.tree.SchemaIterator;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.db.schemaengine.rescon.MemSchemaRegionStatistics;
import org.apache.iotdb.db.schemaengine.schemaregion.read.req.IShowTimeSeriesPlan;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.ITimeSeriesSchemaInfo;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.info.impl.ShowTimeSeriesResult;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.ISchemaReader;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.impl.SchemaReaderLimitOffsetWrapper;
import org.apache.iotdb.db.schemaengine.schemaregion.read.resp.reader.impl.TimeseriesReaderWithViewFetch;

import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.utils.RamUsageEstimator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import static java.util.stream.Collectors.toList;

public class TagManager {

  private static final String TAG_FORMAT = "tag key is %s, tag value is %s, tlog offset is %d";
  private static final String DEBUG_MSG = "%s : TimeSeries %s is removed from tag inverted index, ";
  private static final String DEBUG_MSG_1 =
      "%s: TimeSeries %s's tag info has been removed from tag inverted index ";
  private static final String PREVIOUS_CONDITION =
      "before deleting it, tag key is %s, tag value is %s, tlog offset is %d, contains key %b";

  private static final Logger logger = LoggerFactory.getLogger(TagManager.class);
  private static final CommonConfig COMMON_CONFIG = CommonDescriptor.getInstance().getConfig();

  private TagLogFile tagLogFile;
  // tag key -> tag value -> LeafMNode
  private final Map<String, Map<String, Set<IMeasurementMNode<?>>>> tagIndex =
      new ConcurrentHashMap<>();

  private final MemSchemaRegionStatistics regionStatistics;

  public TagManager(String sgSchemaDirPath, MemSchemaRegionStatistics regionStatistics)
      throws IOException {
    tagLogFile = new TagLogFile(sgSchemaDirPath, SchemaConstant.TAG_LOG);
    this.regionStatistics = regionStatistics;
  }

  public synchronized boolean createSnapshot(final File targetDir) {
    final File tagLogSnapshot =
        SystemFileFactory.INSTANCE.getFile(targetDir, SchemaConstant.TAG_LOG_SNAPSHOT);
    final File tagLogSnapshotTmp =
        SystemFileFactory.INSTANCE.getFile(targetDir, SchemaConstant.TAG_LOG_SNAPSHOT_TMP);
    try {
      tagLogFile.copyTo(tagLogSnapshotTmp);
      if (tagLogSnapshot.exists() && !FileUtils.deleteFileIfExist(tagLogSnapshot)) {
        logger.warn(
            "Failed to delete old snapshot {} while creating tagManager snapshot.",
            tagLogSnapshot.getName());
        return false;
      }
      if (!tagLogSnapshotTmp.renameTo(tagLogSnapshot)) {
        logger.warn(
            "Failed to rename {} to {} while creating tagManager snapshot.",
            tagLogSnapshotTmp.getName(),
            tagLogSnapshot.getName());
        if (!FileUtils.deleteFileIfExist(tagLogSnapshot)) {
          logger.warn("Failed to delete {} after renaming failure.", tagLogSnapshot.getName());
        }
        return false;
      }

      return true;
    } catch (final IOException e) {
      logger.error("Failed to create tagManager snapshot due to {}", e.getMessage(), e);
      if (!FileUtils.deleteFileIfExist(tagLogSnapshot)) {
        logger.warn(
            "Failed to delete {} after creating tagManager snapshot failure.",
            tagLogSnapshot.getName());
      }
      return false;
    } finally {
      if (!FileUtils.deleteFileIfExist(tagLogSnapshotTmp)) {
        logger.warn("Failed to delete {}.", tagLogSnapshotTmp.getName());
      }
    }
  }

  public static TagManager loadFromSnapshot(
      File snapshotDir, String sgSchemaDirPath, MemSchemaRegionStatistics regionStatistics)
      throws IOException {
    File tagSnapshot =
        SystemFileFactory.INSTANCE.getFile(snapshotDir, SchemaConstant.TAG_LOG_SNAPSHOT);
    File tagFile = SystemFileFactory.INSTANCE.getFile(sgSchemaDirPath, SchemaConstant.TAG_LOG);
    if (tagFile.exists() && !tagFile.delete()) {
      logger.warn("Failed to delete existing {} when loading snapshot.", tagFile.getName());
    }

    try {
      org.apache.commons.io.FileUtils.copyFile(tagSnapshot, tagFile);
      return new TagManager(sgSchemaDirPath, regionStatistics);
    } catch (IOException e) {
      if (!tagFile.delete()) {
        logger.warn(
            "Failed to delete existing {} when copying snapshot failure.", tagFile.getName());
      }
      throw e;
    }
  }

  public boolean recoverIndex(long offset, IMeasurementMNode<?> measurementMNode)
      throws IOException {
    Map<String, String> tags = tagLogFile.readTag(offset);
    if (tags == null || tags.isEmpty()) {
      return false;
    } else {
      addIndex(tags, measurementMNode);
      return true;
    }
  }

  public void addIndex(String tagKey, String tagValue, IMeasurementMNode<?> measurementMNode) {
    if (tagKey == null || tagValue == null || measurementMNode == null) {
      return;
    }

    int tagIndexOldSize = tagIndex.size();
    Map<String, Set<IMeasurementMNode<?>>> tagValueMap =
        tagIndex.computeIfAbsent(tagKey, k -> new ConcurrentHashMap<>());
    int tagIndexNewSize = tagIndex.size();

    int tagValueMapOldSize = tagValueMap.size();
    Set<IMeasurementMNode<?>> measurementsSet =
        tagValueMap.computeIfAbsent(tagValue, v -> Collections.synchronizedSet(new HashSet<>()));
    int tagValueMapNewSize = tagValueMap.size();

    int measurementsSetOldSize = measurementsSet.size();
    measurementsSet.add(measurementMNode);
    int measurementsSetNewSize = measurementsSet.size();

    long memorySize = 0;
    if (tagIndexNewSize - tagIndexOldSize == 1) {
      // the last 4 is the memory occupied by the size of tagvaluemap
      memorySize += RamUsageEstimator.sizeOf(tagKey) + 4;
    }
    if (tagValueMapNewSize - tagValueMapOldSize == 1) {
      // the last 4 is the memory occupied by the size of measurementsSet
      memorySize += RamUsageEstimator.sizeOf(tagValue) + 4;
    }
    if (measurementsSetNewSize - measurementsSetOldSize == 1) {
      // 8 is the memory occupied by the length of the IMeasurementMNode
      memorySize += RamUsageEstimator.NUM_BYTES_OBJECT_REF + 4;
    }
    requestMemory(memorySize);
  }

  public void addIndex(Map<String, String> tagsMap, IMeasurementMNode<?> measurementMNode) {
    if (tagsMap != null && measurementMNode != null) {
      for (Map.Entry<String, String> entry : tagsMap.entrySet()) {
        addIndex(entry.getKey(), entry.getValue(), measurementMNode);
      }
    }
  }

  public void removeIndex(String tagKey, String tagValue, IMeasurementMNode<?> measurementMNode) {
    if (tagKey == null || tagValue == null || measurementMNode == null) {
      return;
    }
    // init memory size
    long memorySize = 0;
    if (tagIndex.get(tagKey).get(tagValue).remove(measurementMNode)) {
      memorySize += RamUsageEstimator.NUM_BYTES_OBJECT_REF + 4;
    }
    if (tagIndex.get(tagKey).get(tagValue).isEmpty()) {
      if (tagIndex.get(tagKey).remove(tagValue) != null) {
        // the last 4 is the memory occupied by the size of IMeasurementMNodeSet
        memorySize += RamUsageEstimator.sizeOf(tagValue) + 4;
      }
    }
    if (tagIndex.get(tagKey).isEmpty()) {
      if (tagIndex.remove(tagKey) != null) {
        // the last 4 is the memory occupied by the size of tagValueMap
        memorySize += RamUsageEstimator.sizeOf(tagKey) + 4;
      }
    }
    releaseMemory(memorySize);
  }

  private List<IMeasurementMNode<?>> getMatchedTimeseriesInIndex(TagFilter tagFilter) {
    if (!tagIndex.containsKey(tagFilter.getKey())) {
      return Collections.emptyList();
    }
    Map<String, Set<IMeasurementMNode<?>>> value2Node = tagIndex.get(tagFilter.getKey());
    if (value2Node.isEmpty()) {
      return Collections.emptyList();
    }

    List<IMeasurementMNode<?>> allMatchedNodes = new ArrayList<>();
    if (tagFilter.isContains()) {
      for (Map.Entry<String, Set<IMeasurementMNode<?>>> entry : value2Node.entrySet()) {
        if (entry.getKey() == null || entry.getValue() == null) {
          continue;
        }
        String tagValue = entry.getKey();
        if (tagValue.contains(tagFilter.getValue())) {
          allMatchedNodes.addAll(entry.getValue());
        }
      }
    } else {
      for (Map.Entry<String, Set<IMeasurementMNode<?>>> entry : value2Node.entrySet()) {
        if (entry.getKey() == null || entry.getValue() == null) {
          continue;
        }
        String tagValue = entry.getKey();
        if (tagFilter.getValue().equals(tagValue)) {
          allMatchedNodes.addAll(entry.getValue());
        }
      }
    }
    // we just sort them by the alphabetical order
    allMatchedNodes =
        allMatchedNodes.stream()
            .sorted(Comparator.comparing(IMNode::getFullPath))
            .collect(toList());

    return allMatchedNodes;
  }

  public ISchemaReader<ITimeSeriesSchemaInfo> getTimeSeriesReaderWithIndex(
      IShowTimeSeriesPlan plan) {
    // schemaFilter must not null
    SchemaFilter schemaFilter = plan.getSchemaFilter();
    // currently, only one TagFilter is supported
    // all IMeasurementMNode in allMatchedNodes satisfied TagFilter
    Iterator<IMeasurementMNode<?>> allMatchedNodes =
        getMatchedTimeseriesInIndex(
                (TagFilter) SchemaFilter.extract(schemaFilter, SchemaFilterType.TAGS_FILTER).get(0))
            .iterator();
    PartialPath pathPattern = plan.getPath();
    SchemaIterator<ITimeSeriesSchemaInfo> schemaIterator =
        new SchemaIterator<ITimeSeriesSchemaInfo>() {
          private ITimeSeriesSchemaInfo nextMatched;
          private Throwable throwable;

          @Override
          public boolean hasNext() {
            if (throwable == null && nextMatched == null) {
              try {
                getNext();
              } catch (Throwable e) {
                throwable = e;
              }
            }
            return throwable == null && nextMatched != null;
          }

          @Override
          public ITimeSeriesSchemaInfo next() {
            if (!hasNext()) {
              throw new NoSuchElementException();
            }
            ITimeSeriesSchemaInfo result = nextMatched;
            nextMatched = null;
            return result;
          }

          private void getNext() throws IOException {
            nextMatched = null;
            while (allMatchedNodes.hasNext()) {
              IMeasurementMNode<?> node = allMatchedNodes.next();
              if (plan.isPrefixMatch()
                  ? pathPattern.prefixMatchFullPath(node.getPartialPath())
                  : pathPattern.matchFullPath(node.getPartialPath())) {
                Pair<Map<String, String>, Map<String, String>> tagAndAttributePair =
                    readTagFile(node.getOffset());
                nextMatched =
                    new ShowTimeSeriesResult(
                        node.getFullPath(),
                        node.getAlias(),
                        node.getSchema(),
                        tagAndAttributePair.left,
                        tagAndAttributePair.right,
                        node.getParent().getAsDeviceMNode().isAligned());
                break;
              }
            }
          }

          @Override
          public Throwable getFailure() {
            return throwable;
          }

          @Override
          public boolean isSuccess() {
            return throwable == null;
          }

          @Override
          public void close() {
            // do nothing
          }
        };
    ISchemaReader<ITimeSeriesSchemaInfo> reader =
        new TimeseriesReaderWithViewFetch(schemaIterator, schemaFilter);
    if (plan.getLimit() > 0 || plan.getOffset() > 0) {
      return new SchemaReaderLimitOffsetWrapper<>(reader, plan.getLimit(), plan.getOffset());
    } else {
      return reader;
    }
  }

  /**
   * Remove the node from the tag inverted index.
   *
   * @throws IOException error occurred when reading disk
   */
  public void removeFromTagInvertedIndex(IMeasurementMNode<?> node) throws IOException {
    if (node.getOffset() < 0) {
      return;
    }
    Map<String, String> tagMap = tagLogFile.readTag(node.getOffset());
    if (tagMap != null) {
      for (Map.Entry<String, String> entry : tagMap.entrySet()) {
        if (tagIndex.containsKey(entry.getKey())
            && tagIndex.get(entry.getKey()).containsKey(entry.getValue())) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                String.format(
                    String.format(DEBUG_MSG, "Delete" + TAG_FORMAT, node.getFullPath()),
                    entry.getKey(),
                    entry.getValue(),
                    node.getOffset()));
          }

          removeIndex(entry.getKey(), entry.getValue(), node);

        } else {
          if (logger.isDebugEnabled()) {
            logger.debug(
                String.format(
                    String.format(DEBUG_MSG_1, "Delete" + PREVIOUS_CONDITION, node.getFullPath()),
                    entry.getKey(),
                    entry.getValue(),
                    node.getOffset(),
                    tagIndex.containsKey(entry.getKey())));
          }
        }
      }
    }
  }

  /**
   * Upsert tags and attributes key-value for the timeseries if the key has existed, just use the
   * new value to update it.
   *
   * @throws MetadataException metadata exception
   * @throws IOException error occurred when reading disk
   */
  public void updateTagsAndAttributes(
      final Map<String, String> tagsMap,
      final Map<String, String> attributesMap,
      final IMeasurementMNode<?> leafMNode)
      throws MetadataException, IOException {

    final Pair<Map<String, String>, Map<String, String>> pair =
        tagLogFile.read(leafMNode.getOffset());

    if (tagsMap != null) {
      for (Map.Entry<String, String> entry : tagsMap.entrySet()) {
        String key = entry.getKey();
        String value = entry.getValue();
        String beforeValue = pair.left.get(key);
        pair.left.put(key, value);
        // if the key has existed and the value is not equal to the new one
        // we should remove before key-value from inverted index map
        if (beforeValue != null && !beforeValue.equals(value)) {

          if (tagIndex.containsKey(key) && tagIndex.get(key).containsKey(beforeValue)) {
            if (logger.isDebugEnabled()) {
              logger.debug(
                  String.format(
                      String.format(DEBUG_MSG, "Upsert" + TAG_FORMAT, leafMNode.getFullPath()),
                      key,
                      beforeValue,
                      leafMNode.getOffset()));
            }

            removeIndex(key, beforeValue, leafMNode);

          } else {
            if (logger.isDebugEnabled()) {
              logger.debug(
                  String.format(
                      String.format(
                          DEBUG_MSG_1, "Upsert" + PREVIOUS_CONDITION, leafMNode.getFullPath()),
                      key,
                      beforeValue,
                      leafMNode.getOffset(),
                      tagIndex.containsKey(key)));
            }
          }
        }

        // if the key doesn't exist or the value is not equal to the new one
        // we should add a new key-value to inverted index map
        if (beforeValue == null || !beforeValue.equals(value)) {
          addIndex(key, value, leafMNode);
        }
      }
    }

    if (attributesMap != null) {
      pair.right.putAll(attributesMap);
    }

    // persist the change to disk
    tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());
  }

  /**
   * Add new attributes key-value for the timeseries.
   *
   * @param attributesMap newly added attributes map
   * @throws MetadataException tagLogFile write error or attributes already exist
   * @throws IOException error occurred when reading disk
   */
  public void addAttributes(
      Map<String, String> attributesMap, PartialPath fullPath, IMeasurementMNode<?> leafMNode)
      throws MetadataException, IOException {

    Pair<Map<String, String>, Map<String, String>> pair = tagLogFile.read(leafMNode.getOffset());

    for (Map.Entry<String, String> entry : attributesMap.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (pair.right.containsKey(key)) {
        throw new MetadataException(
            String.format("TimeSeries [%s] already has the attribute [%s].", fullPath, key));
      }
      pair.right.put(key, value);
    }

    // persist the change to disk
    tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());
  }

  /**
   * Add new tags key-value for the timeseries.
   *
   * @param tagsMap newly added tags map
   * @param fullPath timeseries
   * @throws MetadataException tagLogFile write error or tag already exists
   * @throws IOException error occurred when reading disk
   */
  public void addTags(
      Map<String, String> tagsMap, PartialPath fullPath, IMeasurementMNode<?> leafMNode)
      throws MetadataException, IOException {

    Pair<Map<String, String>, Map<String, String>> pair = tagLogFile.read(leafMNode.getOffset());

    for (Map.Entry<String, String> entry : tagsMap.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      if (pair.left.containsKey(key)) {
        throw new MetadataException(
            String.format("TimeSeries [%s] already has the tag [%s].", fullPath, key));
      }
      pair.left.put(key, value);
    }

    // persist the change to disk
    tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());

    // update tag inverted map
    addIndex(tagsMap, leafMNode);
  }

  /**
   * Drop tags or attributes of the timeseries. It will not throw exception even if the key does not
   * exist.
   *
   * @param keySet tags key or attributes key
   * @throws MetadataException metadata exception
   * @throws IOException error occurred when reading disk
   */
  public void dropTagsOrAttributes(
      Set<String> keySet, PartialPath fullPath, IMeasurementMNode<?> leafMNode)
      throws MetadataException, IOException {
    Pair<Map<String, String>, Map<String, String>> pair = tagLogFile.read(leafMNode.getOffset());

    Map<String, String> deleteTag = new HashMap<>();
    for (String key : keySet) {
      // check tag map
      // check attribute map
      String removeVal = pair.left.remove(key);
      if (removeVal != null) {
        deleteTag.put(key, removeVal);
      } else {
        removeVal = pair.right.remove(key);
        if (removeVal == null) {
          logger.warn("TimeSeries [{}] does not have tag/attribute [{}]", fullPath, key);
        }
      }
    }

    // persist the change to disk
    tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());

    if (!deleteTag.isEmpty()) {
      for (Map.Entry<String, String> entry : deleteTag.entrySet()) {
        if (tagIndex.containsKey((entry.getKey()))
            && tagIndex.get(entry.getKey()).containsKey(entry.getValue())) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                String.format(
                    String.format(DEBUG_MSG, "Drop" + TAG_FORMAT, leafMNode.getFullPath()),
                    entry.getKey(),
                    entry.getValue(),
                    leafMNode.getOffset()));
          }

          removeIndex(entry.getKey(), entry.getValue(), leafMNode);

        } else {
          if (logger.isDebugEnabled()) {
            logger.debug(
                String.format(
                    String.format(
                        DEBUG_MSG_1, "Drop" + PREVIOUS_CONDITION, leafMNode.getFullPath()),
                    entry.getKey(),
                    entry.getValue(),
                    leafMNode.getOffset(),
                    tagIndex.containsKey(entry.getKey())));
          }
        }
      }
    }
  }

  /**
   * Set/change the values of tags or attributes.
   *
   * @param alterMap the new tags or attributes key-value
   * @throws MetadataException tagLogFile write error or tags/attributes do not exist
   * @throws IOException error occurred when reading disk
   */
  public void setTagsOrAttributesValue(
      Map<String, String> alterMap, PartialPath fullPath, IMeasurementMNode<?> leafMNode)
      throws MetadataException, IOException {
    // tags, attributes
    Pair<Map<String, String>, Map<String, String>> pair = tagLogFile.read(leafMNode.getOffset());
    Map<String, String> oldTagValue = new HashMap<>();
    Map<String, String> newTagValue = new HashMap<>();

    for (Map.Entry<String, String> entry : alterMap.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      // check tag map
      if (pair.left.containsKey(key)) {
        oldTagValue.put(key, pair.left.get(key));
        newTagValue.put(key, value);
        pair.left.put(key, value);
      } else if (pair.right.containsKey(key)) {
        // check attribute map
        pair.right.put(key, value);
      } else {
        throw new MetadataException(
            String.format("TimeSeries [%s] does not have tag/attribute [%s].", fullPath, key),
            true);
      }
    }

    // persist the change to disk
    tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());

    for (Map.Entry<String, String> entry : oldTagValue.entrySet()) {
      String key = entry.getKey();
      String beforeValue = entry.getValue();
      String currentValue = newTagValue.get(key);
      // change the tag inverted index map
      if (tagIndex.containsKey(key) && tagIndex.get(key).containsKey(beforeValue)) {

        if (logger.isDebugEnabled()) {
          logger.debug(
              String.format(
                  String.format(DEBUG_MSG, "Set" + TAG_FORMAT, leafMNode.getFullPath()),
                  entry.getKey(),
                  beforeValue,
                  leafMNode.getOffset()));
        }

        removeIndex(key, beforeValue, leafMNode);

      } else {
        if (logger.isDebugEnabled()) {
          logger.debug(
              String.format(
                  String.format(DEBUG_MSG_1, "Set" + PREVIOUS_CONDITION, leafMNode.getFullPath()),
                  key,
                  beforeValue,
                  leafMNode.getOffset(),
                  tagIndex.containsKey(key)));
        }
      }
      addIndex(key, currentValue, leafMNode);
    }
  }

  /**
   * Rename the tag or attribute's key of the timeseries.
   *
   * @param oldKey old key of tag or attribute
   * @param newKey new key of tag or attribute
   * @throws MetadataException tagLogFile write error or does not have tag/attribute or already has
   *     a tag/attribute named newKey
   * @throws IOException error occurred when reading disk
   */
  public void renameTagOrAttributeKey(
      String oldKey, String newKey, PartialPath fullPath, IMeasurementMNode<?> leafMNode)
      throws MetadataException, IOException {
    // tags, attributes
    Pair<Map<String, String>, Map<String, String>> pair = tagLogFile.read(leafMNode.getOffset());

    // current name has existed
    if (pair.left.containsKey(newKey) || pair.right.containsKey(newKey)) {
      throw new MetadataException(
          String.format(
              "TimeSeries [%s] already has a tag/attribute named [%s].", fullPath, newKey),
          true);
    }

    // check tag map
    if (pair.left.containsKey(oldKey)) {
      String value = pair.left.remove(oldKey);
      pair.left.put(newKey, value);
      // persist the change to disk
      tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());
      // change the tag inverted index map
      if (tagIndex.containsKey(oldKey) && tagIndex.get(oldKey).containsKey(value)) {

        if (logger.isDebugEnabled()) {
          logger.debug(
              String.format(
                  String.format(DEBUG_MSG, "Rename" + TAG_FORMAT, leafMNode.getFullPath()),
                  oldKey,
                  value,
                  leafMNode.getOffset()));
        }

        removeIndex(oldKey, value, leafMNode);

      } else {
        if (logger.isDebugEnabled()) {
          logger.debug(
              String.format(
                  String.format(
                      DEBUG_MSG_1, "Rename" + PREVIOUS_CONDITION, leafMNode.getFullPath()),
                  oldKey,
                  value,
                  leafMNode.getOffset(),
                  tagIndex.containsKey(oldKey)));
        }
      }
      addIndex(newKey, value, leafMNode);
    } else if (pair.right.containsKey(oldKey)) {
      // check attribute map
      pair.right.put(newKey, pair.right.remove(oldKey));
      // persist the change to disk
      tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());
    } else {
      throw new MetadataException(
          String.format("TimeSeries [%s] does not have tag/attribute [%s].", fullPath, oldKey),
          true);
    }
  }

  public long writeTagFile(Map<String, String> tags, Map<String, String> attributes)
      throws MetadataException, IOException {
    return tagLogFile.write(tags, attributes);
  }

  public Pair<Map<String, String>, Map<String, String>> readTagFile(long tagFileOffset)
      throws IOException {
    return tagLogFile.read(tagFileOffset);
  }

  /**
   * Read the tags of this node.
   *
   * @param node the node to query.
   * @return the tag key-value map.
   * @throws RuntimeException If any IOException happens.
   */
  public Map<String, String> readTags(IMeasurementMNode<?> node) {
    try {
      return readTagFile(node.getOffset()).getLeft();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  /**
   * Read the attributes of this node.
   *
   * @param node the node to query.
   * @return the attribute key-value map.
   * @throws RuntimeException If any IOException happens.
   */
  public Map<String, String> readAttributes(IMeasurementMNode<?> node) {
    try {
      return readTagFile(node.getOffset()).getRight();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  public void clear() throws IOException {
    this.tagIndex.clear();
    if (tagLogFile != null) {
      tagLogFile.close();
      tagLogFile = null;
    }
  }

  private void requestMemory(long size) {
    if (regionStatistics != null) {
      regionStatistics.requestMemory(size);
    }
  }

  private void releaseMemory(long size) {
    if (regionStatistics != null) {
      regionStatistics.releaseMemory(size);
    }
  }
}
