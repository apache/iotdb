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
package org.apache.iotdb.db.metadata.tag;

import org.apache.iotdb.db.conf.IoTDBConfig;
import org.apache.iotdb.db.conf.IoTDBDescriptor;
import org.apache.iotdb.db.engine.StorageEngine;
import org.apache.iotdb.db.engine.storagegroup.StorageGroupProcessor;
import org.apache.iotdb.db.exception.StorageEngineException;
import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.MetadataConstant;
import org.apache.iotdb.db.metadata.PartialPath;
import org.apache.iotdb.db.metadata.lastCache.LastCacheManager;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.mnode.IMeasurementMNode;
import org.apache.iotdb.db.qp.physical.sys.ShowTimeSeriesPlan;
import org.apache.iotdb.db.query.context.QueryContext;
import org.apache.iotdb.db.utils.TestOnly;
import org.apache.iotdb.tsfile.utils.Pair;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
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
  private static IoTDBConfig config = IoTDBDescriptor.getInstance().getConfig();

  private TagLogFile tagLogFile;
  // tag key -> tag value -> LeafMNode
  private Map<String, Map<String, Set<IMeasurementMNode>>> tagIndex = new ConcurrentHashMap<>();

  private static class TagManagerHolder {

    private TagManagerHolder() {
      // allowed to do nothing
    }

    private static final TagManager INSTANCE = new TagManager();
  }

  public static TagManager getInstance() {
    return TagManagerHolder.INSTANCE;
  }

  @TestOnly
  public static TagManager getNewInstanceForTest() {
    return new TagManager();
  }

  private TagManager() {}

  public void init() throws IOException {
    tagLogFile = new TagLogFile(config.getSchemaDir(), MetadataConstant.TAG_LOG);
  }

  public void addIndex(String tagKey, String tagValue, IMeasurementMNode measurementMNode) {
    tagIndex
        .computeIfAbsent(tagKey, k -> new ConcurrentHashMap<>())
        .computeIfAbsent(tagValue, v -> Collections.synchronizedSet(new HashSet<>()))
        .add(measurementMNode);
  }

  public void addIndex(Map<String, String> tagsMap, IMeasurementMNode measurementMNode) {
    if (tagsMap != null && measurementMNode != null) {
      for (Map.Entry<String, String> entry : tagsMap.entrySet()) {
        addIndex(entry.getKey(), entry.getValue(), measurementMNode);
      }
    }
  }

  public void removeIndex(String tagKey, String tagValue, IMeasurementMNode measurementMNode) {
    tagIndex.get(tagKey).get(tagValue).remove(measurementMNode);
    if (tagIndex.get(tagKey).get(tagValue).isEmpty()) {
      tagIndex.get(tagKey).remove(tagValue);
    }
  }

  public List<IMeasurementMNode> getMatchedTimeseriesInIndex(
      ShowTimeSeriesPlan plan, QueryContext context) throws MetadataException {
    if (!tagIndex.containsKey(plan.getKey())) {
      throw new MetadataException("The key " + plan.getKey() + " is not a tag.", true);
    }
    Map<String, Set<IMeasurementMNode>> value2Node = tagIndex.get(plan.getKey());
    if (value2Node.isEmpty()) {
      throw new MetadataException("The key " + plan.getKey() + " is not a tag.");
    }

    List<IMeasurementMNode> allMatchedNodes = new ArrayList<>();
    if (plan.isContains()) {
      for (Map.Entry<String, Set<IMeasurementMNode>> entry : value2Node.entrySet()) {
        if (entry.getKey() == null || entry.getValue() == null) {
          continue;
        }
        String tagValue = entry.getKey();
        if (tagValue.contains(plan.getValue())) {
          allMatchedNodes.addAll(entry.getValue());
        }
      }
    } else {
      for (Map.Entry<String, Set<IMeasurementMNode>> entry : value2Node.entrySet()) {
        if (entry.getKey() == null || entry.getValue() == null) {
          continue;
        }
        String tagValue = entry.getKey();
        if (plan.getValue().equals(tagValue)) {
          allMatchedNodes.addAll(entry.getValue());
        }
      }
    }

    // if ordered by heat, we sort all the timeseries by the descending order of the last insert
    // timestamp
    if (plan.isOrderByHeat()) {
      List<StorageGroupProcessor> list;
      try {
        list =
            StorageEngine.getInstance()
                .mergeLock(allMatchedNodes.stream().map(IMNode::getPartialPath).collect(toList()));
        try {
          allMatchedNodes =
              allMatchedNodes.stream()
                  .sorted(
                      Comparator.comparingLong(
                              (IMeasurementMNode mNode) ->
                                  LastCacheManager.getLastTimeStamp(mNode, context))
                          .reversed()
                          .thenComparing(IMNode::getFullPath))
                  .collect(toList());
        } finally {
          StorageEngine.getInstance().mergeUnLock(list);
        }
      } catch (StorageEngineException e) {
        throw new MetadataException(e);
      }
    } else {
      // otherwise, we just sort them by the alphabetical order
      allMatchedNodes =
          allMatchedNodes.stream()
              .sorted(Comparator.comparing(IMNode::getFullPath))
              .collect(toList());
    }

    return allMatchedNodes;
  }

  /** remove the node from the tag inverted index */
  public void removeFromTagInvertedIndex(IMeasurementMNode node) throws IOException {
    if (node.getOffset() < 0) {
      return;
    }
    Map<String, String> tagMap =
        tagLogFile.readTag(config.getTagAttributeTotalSize(), node.getOffset());
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
          tagIndex.get(entry.getKey()).get(entry.getValue()).remove(node);
          if (tagIndex.get(entry.getKey()).get(entry.getValue()).isEmpty()) {
            tagIndex.get(entry.getKey()).remove(entry.getValue());
            if (tagIndex.get(entry.getKey()).isEmpty()) {
              tagIndex.remove(entry.getKey());
            }
          }
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
   * upsert tags and attributes key-value for the timeseries if the key has existed, just use the
   * new value to update it.
   */
  public void updateTagsAndAttributes(
      Map<String, String> tagsMap, Map<String, String> attributesMap, IMeasurementMNode leafMNode)
      throws MetadataException, IOException {

    Pair<Map<String, String>, Map<String, String>> pair =
        tagLogFile.read(config.getTagAttributeTotalSize(), leafMNode.getOffset());

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
   * add new attributes key-value for the timeseries
   *
   * @param attributesMap newly added attributes map
   */
  public void addAttributes(
      Map<String, String> attributesMap, PartialPath fullPath, IMeasurementMNode leafMNode)
      throws MetadataException, IOException {

    Pair<Map<String, String>, Map<String, String>> pair =
        tagLogFile.read(config.getTagAttributeTotalSize(), leafMNode.getOffset());

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
   * add new tags key-value for the timeseries
   *
   * @param tagsMap newly added tags map
   * @param fullPath timeseries
   */
  public void addTags(
      Map<String, String> tagsMap, PartialPath fullPath, IMeasurementMNode leafMNode)
      throws MetadataException, IOException {

    Pair<Map<String, String>, Map<String, String>> pair =
        tagLogFile.read(config.getTagAttributeTotalSize(), leafMNode.getOffset());

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
   * drop tags or attributes of the timeseries
   *
   * @param keySet tags key or attributes key
   */
  public void dropTagsOrAttributes(
      Set<String> keySet, PartialPath fullPath, IMeasurementMNode leafMNode)
      throws MetadataException, IOException {
    Pair<Map<String, String>, Map<String, String>> pair =
        tagLogFile.read(config.getTagAttributeTotalSize(), leafMNode.getOffset());

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

    Map<String, Set<IMeasurementMNode>> tagVal2LeafMNodeSet;
    Set<IMeasurementMNode> MMNodes;
    for (Map.Entry<String, String> entry : deleteTag.entrySet()) {
      String key = entry.getKey();
      String value = entry.getValue();
      // change the tag inverted index map
      tagVal2LeafMNodeSet = tagIndex.get(key);
      if (tagVal2LeafMNodeSet != null) {
        MMNodes = tagVal2LeafMNodeSet.get(value);
        if (MMNodes != null) {
          if (logger.isDebugEnabled()) {
            logger.debug(
                String.format(
                    String.format(DEBUG_MSG, "Drop" + TAG_FORMAT, leafMNode.getFullPath()),
                    entry.getKey(),
                    entry.getValue(),
                    leafMNode.getOffset()));
          }

          MMNodes.remove(leafMNode);
          if (MMNodes.isEmpty()) {
            tagVal2LeafMNodeSet.remove(value);
            if (tagVal2LeafMNodeSet.isEmpty()) {
              tagIndex.remove(key);
            }
          }
        }
      } else {
        if (logger.isDebugEnabled()) {
          logger.debug(
              String.format(
                  String.format(DEBUG_MSG_1, "Drop" + PREVIOUS_CONDITION, leafMNode.getFullPath()),
                  key,
                  value,
                  leafMNode.getOffset(),
                  tagIndex.containsKey(key)));
        }
      }
    }
  }

  /**
   * set/change the values of tags or attributes
   *
   * @param alterMap the new tags or attributes key-value
   */
  public void setTagsOrAttributesValue(
      Map<String, String> alterMap, PartialPath fullPath, IMeasurementMNode leafMNode)
      throws MetadataException, IOException {
    // tags, attributes
    Pair<Map<String, String>, Map<String, String>> pair =
        tagLogFile.read(config.getTagAttributeTotalSize(), leafMNode.getOffset());
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

        tagIndex.get(key).get(beforeValue).remove(leafMNode);
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
   * rename the tag or attribute's key of the timeseries
   *
   * @param oldKey old key of tag or attribute
   * @param newKey new key of tag or attribute
   */
  public void renameTagOrAttributeKey(
      String oldKey, String newKey, PartialPath fullPath, IMeasurementMNode leafMNode)
      throws MetadataException, IOException {
    // tags, attributes
    Pair<Map<String, String>, Map<String, String>> pair =
        tagLogFile.read(config.getTagAttributeTotalSize(), leafMNode.getOffset());

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

        tagIndex.get(oldKey).get(value).remove(leafMNode);

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
    return tagLogFile.read(config.getTagAttributeTotalSize(), tagFileOffset);
  }

  public void clear() throws IOException {
    this.tagIndex.clear();
    if (tagLogFile != null) {
      tagLogFile.close();
      tagLogFile = null;
    }
  }
}
