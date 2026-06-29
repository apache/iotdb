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
import org.apache.iotdb.db.i18n.DataNodeSchemaMessages;
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

  // The tag index memory model adds one int-sized estimated overhead for each indexed key, value,
  // and measurement reference. This is an accounting estimate rather than a specific
  // ConcurrentHashMap or Set field.
  private static final long INDEX_ENTRY_OVERHEAD_ESTIMATE_BYTES = Integer.BYTES;
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
            DataNodeSchemaMessages.FAILED_TO_DELETE_OLD_TAG_SNAPSHOT, tagLogSnapshot.getName());
        return false;
      }
      if (!tagLogSnapshotTmp.renameTo(tagLogSnapshot)) {
        logger.warn(
            DataNodeSchemaMessages.FAILED_TO_RENAME_TAG_SNAPSHOT,
            tagLogSnapshotTmp.getName(),
            tagLogSnapshot.getName());
        if (!FileUtils.deleteFileIfExist(tagLogSnapshot)) {
          logger.warn(
              DataNodeSchemaMessages.FAILED_TO_DELETE_AFTER_RENAME_FAILURE,
              tagLogSnapshot.getName());
        }
        return false;
      }

      return true;
    } catch (final IOException e) {
      logger.error(DataNodeSchemaMessages.FAILED_TO_CREATE_TAG_SNAPSHOT, e.getMessage(), e);
      if (!FileUtils.deleteFileIfExist(tagLogSnapshot)) {
        logger.warn(
            DataNodeSchemaMessages.FAILED_TO_DELETE_AFTER_TAG_SNAPSHOT_FAILURE,
            tagLogSnapshot.getName());
      }
      return false;
    } finally {
      if (!FileUtils.deleteFileIfExist(tagLogSnapshotTmp)) {
        logger.warn(DataNodeSchemaMessages.FAILED_TO_DELETE_FILE, tagLogSnapshotTmp.getName());
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
      logger.warn(DataNodeSchemaMessages.FAILED_TO_DELETE_EXISTING_WHEN_LOADING, tagFile.getName());
    }

    try {
      org.apache.tsfile.external.commons.io.FileUtils.copyFile(tagSnapshot, tagFile);
      return new TagManager(sgSchemaDirPath, regionStatistics);
    } catch (IOException e) {
      if (!tagFile.delete()) {
        logger.warn(
            DataNodeSchemaMessages.FAILED_TO_DELETE_EXISTING_WHEN_COPY_FAILURE, tagFile.getName());
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

    tagIndex.compute(
        tagKey,
        (key, tagValueMap) -> {
          long memorySize = 0;
          if (tagValueMap == null) {
            tagValueMap = new ConcurrentHashMap<>();
            memorySize += RamUsageEstimator.sizeOf(tagKey) + INDEX_ENTRY_OVERHEAD_ESTIMATE_BYTES;
          }

          Set<IMeasurementMNode<?>> measurementsSet = tagValueMap.get(tagValue);
          if (measurementsSet == null) {
            measurementsSet = ConcurrentHashMap.newKeySet();
            tagValueMap.put(tagValue, measurementsSet);
            memorySize += RamUsageEstimator.sizeOf(tagValue) + INDEX_ENTRY_OVERHEAD_ESTIMATE_BYTES;
          }

          if (measurementsSet.add(measurementMNode)) {
            memorySize +=
                RamUsageEstimator.NUM_BYTES_OBJECT_REF + INDEX_ENTRY_OVERHEAD_ESTIMATE_BYTES;
          }
          if (memorySize > 0) {
            requestMemory(memorySize);
          }
          return tagValueMap;
        });
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
    tagIndex.computeIfPresent(
        tagKey,
        (key, tagValueMap) -> {
          long memorySize = 0;
          Set<IMeasurementMNode<?>> measurementsSet = tagValueMap.get(tagValue);
          if (measurementsSet == null) {
            return tagValueMap;
          }

          if (measurementsSet.remove(measurementMNode)) {
            memorySize +=
                RamUsageEstimator.NUM_BYTES_OBJECT_REF + INDEX_ENTRY_OVERHEAD_ESTIMATE_BYTES;
          }
          if (measurementsSet.isEmpty()) {
            if (tagValueMap.remove(tagValue, measurementsSet)) {
              memorySize +=
                  RamUsageEstimator.sizeOf(tagValue) + INDEX_ENTRY_OVERHEAD_ESTIMATE_BYTES;
            }
          }
          if (tagValueMap.isEmpty()) {
            memorySize += RamUsageEstimator.sizeOf(tagKey) + INDEX_ENTRY_OVERHEAD_ESTIMATE_BYTES;
            if (memorySize > 0) {
              releaseMemory(memorySize);
            }
            return null;
          }
          if (memorySize > 0) {
            releaseMemory(memorySize);
          }
          return tagValueMap;
        });
  }

  private boolean containsIndex(String tagKey, String tagValue) {
    Map<String, Set<IMeasurementMNode<?>>> tagValueMap = tagIndex.get(tagKey);
    return tagValueMap != null && tagValueMap.containsKey(tagValue);
  }

  private List<IMeasurementMNode<?>> getMatchedTimeseriesInIndex(TagFilter tagFilter) {
    Map<String, Set<IMeasurementMNode<?>>> value2Node = tagIndex.get(tagFilter.getKey());
    if (value2Node == null || value2Node.isEmpty()) {
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
      final IShowTimeSeriesPlan plan) {
    // schemaFilter must not null
    final SchemaFilter schemaFilter = plan.getSchemaFilter();
    // currently, only one TagFilter is supported
    // all IMeasurementMNode in allMatchedNodes satisfied TagFilter
    final Iterator<IMeasurementMNode<?>> allMatchedNodes =
        getMatchedTimeseriesInIndex(
                (TagFilter) SchemaFilter.extract(schemaFilter, SchemaFilterType.TAGS_FILTER).get(0))
            .iterator();
    final PartialPath pathPattern = plan.getPath();
    final SchemaIterator<ITimeSeriesSchemaInfo> schemaIterator =
        new SchemaIterator<ITimeSeriesSchemaInfo>() {
          private ITimeSeriesSchemaInfo nextMatched;
          private Throwable throwable;

          @Override
          public boolean hasNext() {
            if (throwable == null && nextMatched == null) {
              try {
                getNext();
              } catch (final Throwable e) {
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
            final ITimeSeriesSchemaInfo result = nextMatched;
            nextMatched = null;
            return result;
          }

          private void getNext() throws IOException {
            nextMatched = null;
            while (allMatchedNodes.hasNext()) {
              final IMeasurementMNode<?> node = allMatchedNodes.next();
              if (plan.isPrefixMatch()
                  ? pathPattern.prefixMatchFullPath(node.getPartialPath())
                  : pathPattern.matchFullPath(node.getPartialPath())) {
                final Pair<Map<String, String>, Map<String, String>> tagAndAttributePair =
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
    final ISchemaReader<ITimeSeriesSchemaInfo> reader =
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
        if (containsIndex(entry.getKey(), entry.getValue())) {
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

          if (containsIndex(key, beforeValue)) {
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
            String.format(DataNodeSchemaMessages.TIMESERIES_ALREADY_HAS_ATTRIBUTE, fullPath, key));
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
            String.format(DataNodeSchemaMessages.TIMESERIES_ALREADY_HAS_TAG, fullPath, key));
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
          logger.warn(DataNodeSchemaMessages.TIMESERIES_NO_TAG_ATTRIBUTE_LOG, fullPath, key);
        }
      }
    }

    // persist the change to disk
    tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());

    if (!deleteTag.isEmpty()) {
      for (Map.Entry<String, String> entry : deleteTag.entrySet()) {
        if (containsIndex(entry.getKey(), entry.getValue())) {
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
            String.format(
                DataNodeSchemaMessages.TIMESERIES_NO_SPECIFIC_TAG_ATTRIBUTE_FMT, fullPath, key),
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
      if (containsIndex(key, beforeValue)) {

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
              DataNodeSchemaMessages.TIMESERIES_ALREADY_HAS_TAG_ATTRIBUTE_NAMED, fullPath, newKey),
          true);
    }

    // check tag map
    if (pair.left.containsKey(oldKey)) {
      String value = pair.left.remove(oldKey);
      pair.left.put(newKey, value);
      // persist the change to disk
      tagLogFile.write(pair.left, pair.right, leafMNode.getOffset());
      // change the tag inverted index map
      if (containsIndex(oldKey, value)) {

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
          String.format(
              DataNodeSchemaMessages.TIMESERIES_NO_SPECIFIC_TAG_ATTRIBUTE_FMT, fullPath, oldKey),
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
