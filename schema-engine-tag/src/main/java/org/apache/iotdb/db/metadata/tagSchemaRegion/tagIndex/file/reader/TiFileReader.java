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
package org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.reader;

import org.apache.iotdb.db.metadata.tagSchemaRegion.config.SchemaRegionConstant;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.ChunkIndex;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.ChunkIndexEntry;
import org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.TiFileHeader;
import org.apache.iotdb.lsm.sstable.bplustree.entry.BPlusTreeEntry;
import org.apache.iotdb.lsm.sstable.bplustree.reader.BPlusTreeReader;
import org.apache.iotdb.lsm.sstable.fileIO.FileInput;
import org.apache.iotdb.lsm.sstable.fileIO.IFileInput;
import org.apache.iotdb.lsm.sstable.interator.IDiskIterator;
import org.apache.iotdb.lsm.util.BloomFilter;

import org.roaringbitmap.RoaringBitmap;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.TreeMap;

/** Used to read all tiFile-related structures, and supports iterative acquisition of ids */
public class TiFileReader implements IDiskIterator<Integer> {

  private final IFileInput tiFileInput;

  private File file;

  private Iterator<Integer> oneChunkDeviceIDsIterator;

  private List<ChunkIndex> chunkIndices;

  private Integer nextID;

  private int index;

  private long tagKeyIndexOffset;

  private TiFileHeader tiFileHeader;

  private BloomFilter bloomFilter;

  private Map<String, String> tags;

  public TiFileReader(File file, Map<String, String> tags) throws IOException {
    this.file = file;
    this.tiFileInput = new FileInput(file);
    this.tags = tags;
  }

  public TiFileReader(IFileInput tiFileInput, long tagKeyIndexOffset, Map<String, String> tags)
      throws IOException {
    this.tiFileInput = tiFileInput;
    tiFileInput.position(tagKeyIndexOffset);
    this.tagKeyIndexOffset = tagKeyIndexOffset;
    this.tags = tags;
  }

  /**
   * Read the header of the tiFile
   *
   * @return a {@link org.apache.iotdb.db.metadata.tagSchemaRegion.tagIndex.file.entry.TiFileHeader
   *     tiFile header}
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public TiFileHeader readTiFileHeader() throws IOException {
    long length = file.length();
    long startOffset = length - TiFileHeader.getSerializeSize();
    TiFileHeader tiFileHeader = new TiFileHeader();
    tiFileInput.position(startOffset);
    tiFileInput.read(tiFileHeader);
    return tiFileHeader;
  }

  /**
   * Read bloom filter from the specified location in the file
   *
   * @param bloomFilterOffset a non-negative integer counting the number of bytes from the beginning
   *     of the TiFile
   * @return a {@link org.apache.iotdb.lsm.util.BloomFilter bloom filter}bloom filter instance
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public BloomFilter readBloomFilter(long bloomFilterOffset) throws IOException {
    tiFileInput.position(bloomFilterOffset);
    BloomFilter bloomFilter = new BloomFilter();
    tiFileInput.read(bloomFilter);
    return bloomFilter;
  }

  /**
   * Read all ids according to tags, this method does not use iterators, you need to pay attention
   * to memory
   *
   * @param tags Multiple pairs of tags, the returned result is the intersection of multiple pairs
   *     of tags
   * @param tagKeyIndexOffset a non-negative integer counting the number of bytes from the beginning
   *     of the TiFile
   * @return a {@link org.roaringbitmap.RoaringBitmap roaring bitmap} instance
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public RoaringBitmap readAllDeviceID(Map<String, String> tags, long tagKeyIndexOffset)
      throws IOException {
    List<Long> chunkIndexOffsets = getChunkIndexOffsets(tags, tagKeyIndexOffset);
    if (chunkIndexOffsets.size() == 0) {
      return new RoaringBitmap();
    }
    chunkIndexOffsets.sort((o1, o2) -> Long.compare(o2, o1));
    ChunkGroupReader chunkGroupReader = new ChunkGroupReader(tiFileInput);
    RoaringBitmap roaringBitmap = chunkGroupReader.readAllDeviceID(chunkIndexOffsets.get(0));

    for (int i = 1; i < chunkIndexOffsets.size(); i++) {
      chunkGroupReader = new ChunkGroupReader(tiFileInput);
      roaringBitmap.and(chunkGroupReader.readAllDeviceID(chunkIndexOffsets.get(i)));
    }
    return roaringBitmap;
  }

  /**
   * Only read the ids in a chunk that returns the tags condition, use an iterator, and the maximum
   * memory usage does not exceed the maximum memory of a chunk
   *
   * @param chunkIndices The chunk index of all tags is saved
   * @param index Which chunk index entry is currently read
   * @return a {@link org.roaringbitmap.RoaringBitmap roaring bitmap} instance
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public RoaringBitmap readOneChunkDeviceID(List<ChunkIndex> chunkIndices, int index)
      throws IOException {
    if (chunkIndices.size() == 0) {
      return new RoaringBitmap();
    }
    chunkIndices.sort(Comparator.comparingInt(ChunkIndex::getAllCount));
    ChunkIndex baseChunkIndex = chunkIndices.get(0);
    if (index >= baseChunkIndex.getChunkIndexEntries().size()) {
      return new RoaringBitmap();
    }
    ChunkReader chunkReader = new ChunkReader(tiFileInput);
    ChunkIndexEntry baseChunkIndexEntry = baseChunkIndex.getChunkIndexEntries().get(index);
    RoaringBitmap roaringBitmap = chunkReader.readRoaringBitmap(baseChunkIndexEntry.getOffset());
    if (chunkIndices.size() == 1) {
      return roaringBitmap;
    }
    RoaringBitmap deviceIDs = new RoaringBitmap();
    for (int i = 1; i < chunkIndices.size(); i++) {
      List<ChunkIndexEntry> chunkIndexEntries = chunkIndices.get(i).getChunkIndexEntries();
      for (ChunkIndexEntry chunkIndexEntry : chunkIndexEntries) {
        if (chunkIndexEntry.intersect(baseChunkIndexEntry)) {
          chunkReader = new ChunkReader(tiFileInput, chunkIndexEntry.getOffset());
          while (chunkReader.hasNext()) {
            int deviceID = chunkReader.next();
            if (roaringBitmap.contains(deviceID)) {
              deviceIDs.add(deviceID);
            }
          }
        }
      }
    }
    return deviceIDs;
  }

  /**
   * According to the tags and tag key index offset, read all the chunk indexes that meet the
   * conditions, each tag corresponds to a chunk index, if some tags do not find the chunk index,
   * return empty
   *
   * @param tags Multiple pairs of tags, the returned result is the intersection of multiple pairs
   *     of tags
   * @param tagKeyIndexOffset a non-negative integer counting the number of bytes from the beginning
   *     of the TiFile
   * @return A list saves all chunk indexes
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public List<ChunkIndex> getChunkIndices(Map<String, String> tags, long tagKeyIndexOffset)
      throws IOException {
    List<Long> chunkIndexOffsets = getChunkIndexOffsets(tags, tagKeyIndexOffset);
    List<ChunkIndex> chunkIndices = new ArrayList<>();
    for (long chunkIndexOffset : chunkIndexOffsets) {
      ChunkGroupReader chunkGroupReader = new ChunkGroupReader(tiFileInput);
      chunkIndices.add(chunkGroupReader.readChunkIndex(chunkIndexOffset));
    }
    return chunkIndices;
  }

  /**
   * According to the tags and tag key index offset, read all the chunk indexes offset that meet the
   * conditions, each tag corresponds to a chunk index, if some tags do not find the chunk index,
   * return empty
   *
   * @param tags Multiple pairs of tags, the returned result is the intersection of multiple pairs
   *     of tags
   * @param tagKeyIndexOffset a non-negative integer counting the number of bytes from the beginning
   *     of the TiFile
   * @return A list saves all chunk indexes offset
   * @throws IOException Signals that an I/O exception has occurred.
   */
  public List<Long> getChunkIndexOffsets(Map<String, String> tags, long tagKeyIndexOffset)
      throws IOException {
    Map<Long, String> tagValueIndexOffsets =
        getTagValueIndexOffsets(tags.keySet(), tagKeyIndexOffset);
    if (tagValueIndexOffsets.keySet().size() < tags.keySet().size()) {
      return new ArrayList<>();
    }
    TreeMap<Long, String> tagValueAndOffsets = new TreeMap<>((o1, o2) -> Long.compare(o2, o1));
    tagValueIndexOffsets.forEach((key, value) -> tagValueAndOffsets.put(key, tags.get(value)));
    List<Long> chunkIndexOffsets = new ArrayList<>();
    for (Map.Entry<Long, String> entry : tagValueAndOffsets.entrySet()) {
      long offset = getChunkIndexOffset(entry.getValue(), entry.getKey());
      if (offset == -1) {
        return new ArrayList<>();
      }
      chunkIndexOffsets.add(offset);
    }
    return chunkIndexOffsets;
  }

  /**
   * According to the tag key index offset, read the tag key index, and obtain the first address of
   * the tag value index corresponding to each tag value
   *
   * @param tagKeys all tag keys
   * @param tagKeyIndexOffset a non-negative integer counting the number of bytes from the beginning
   *     of the TiFile
   * @return A map, the key is the offset of the tag value index, and the value is the tag value to
   *     be searched on the tag value index
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private Map<Long, String> getTagValueIndexOffsets(Set<String> tagKeys, long tagKeyIndexOffset)
      throws IOException {
    BPlusTreeReader bPlusTreeReader = new BPlusTreeReader(tiFileInput, tagKeyIndexOffset);
    List<BPlusTreeEntry> bPlusTreeEntries = bPlusTreeReader.getBPlusTreeEntries(tagKeys);
    Map<Long, String> offsets = new HashMap<>();
    bPlusTreeEntries.forEach(
        bPlusTreeEntry -> offsets.put(bPlusTreeEntry.getOffset(), bPlusTreeEntry.getName()));
    return offsets;
  }

  /**
   * According to the tag value index offset, find the offset of the corresponding chunk index on
   * the tag value index according to the tag value
   *
   * @param tagValue tag value
   * @param tagValueIndexOffset a non-negative integer counting the number of bytes from the
   *     beginning of the TiFile
   * @return The chunk index corresponding to the tag value
   * @throws IOException Signals that an I/O exception has occurred.
   */
  private long getChunkIndexOffset(String tagValue, long tagValueIndexOffset) throws IOException {
    BPlusTreeReader bPlusTreeReader = new BPlusTreeReader(tiFileInput, tagValueIndexOffset);
    Set<String> tagValues = new HashSet<>();
    tagValues.add(tagValue);
    List<BPlusTreeEntry> bPlusTreeEntries = bPlusTreeReader.getBPlusTreeEntries(tagValues);
    if (bPlusTreeEntries.size() == 0) {
      return -1;
    }
    return bPlusTreeEntries.get(0).getOffset();
  }

  /**
   * Determine whether all tags are included in the Bloom filter
   *
   * @param tags Multiple pairs of tags
   * @return If there is any tag, the Bloom filter does not contain it, then return false
   */
  private boolean bloomFilterHas(Map<String, String> tags) {
    for (Map.Entry<String, String> tag : tags.entrySet()) {
      if (bloomFilter.contains(tag.getKey() + SchemaRegionConstant.SEPARATOR + tag.getValue())) {
        continue;
      } else {
        return false;
      }
    }
    return true;
  }

  @Override
  public boolean hasNext() throws IOException {
    if (nextID != null) {
      return true;
    }
    if (tiFileHeader == null) {
      tiFileHeader = readTiFileHeader();
    }
    if (bloomFilter == null) {
      bloomFilter = readBloomFilter(tiFileHeader.getBloomFilterOffset());
    }
    if (!bloomFilterHas(tags)) {
      return false;
    }
    if (chunkIndices == null) {
      chunkIndices = getChunkIndices(tags, tiFileHeader.getTagKeyIndexOffset());
      if (chunkIndices.size() == 0) {
        return false;
      }
      chunkIndices.sort(Comparator.comparingInt(ChunkIndex::getAllCount));
    }
    if (oneChunkDeviceIDsIterator != null) {
      if (oneChunkDeviceIDsIterator.hasNext()) {
        nextID = oneChunkDeviceIDsIterator.next();
        return true;
      } else {
        oneChunkDeviceIDsIterator = null;
        index++;
      }
    }
    while (index < chunkIndices.get(0).getChunkIndexEntries().size()) {
      RoaringBitmap deviceIDs = readOneChunkDeviceID(chunkIndices, index);
      oneChunkDeviceIDsIterator = deviceIDs.iterator();
      if (oneChunkDeviceIDsIterator.hasNext()) {
        nextID = oneChunkDeviceIDsIterator.next();
        return true;
      }
      index++;
    }
    return false;
  }

  @Override
  public Integer next() throws IOException {
    if (nextID == null) {
      throw new NoSuchElementException();
    }
    int nowId = nextID;
    nextID = null;
    return nowId;
  }

  public void close() throws IOException {
    tiFileInput.close();
  }
}
