package org.apache.iotdb.db.pipe.connector.util;

import org.apache.iotdb.pipe.api.exception.PipeException;

import org.apache.commons.io.FileUtils;
import org.apache.tsfile.exception.write.WriteProcessException;
import org.apache.tsfile.file.metadata.IDeviceID;
import org.apache.tsfile.file.metadata.TableSchema;
import org.apache.tsfile.utils.Pair;
import org.apache.tsfile.write.record.Tablet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

public class PipeTableModeTsFileBuilder extends PipeTsFileBuilder {

  private static final Logger LOGGER = LoggerFactory.getLogger(PipeTableModeTsFileBuilder.class);

  private final Map<String, List<Tablet>> dataBase2TabletList = new HashMap<>();
  private final Map<String, List<List<Pair<IDeviceID, Integer>>>> tabletDeviceIdTimeIndex =
      new HashMap<>();

  public PipeTableModeTsFileBuilder(AtomicLong currentBatchId, AtomicLong tsFileIdGenerator) {
    super(currentBatchId, tsFileIdGenerator);
  }

  @Override
  public void bufferTableModelTablet(
      String dataBase, Tablet tablet, List<Pair<IDeviceID, Integer>> deviceID2Index) {
    dataBase2TabletList.computeIfAbsent(dataBase, db -> new ArrayList<>()).add(tablet);
    tabletDeviceIdTimeIndex.computeIfAbsent(dataBase, db -> new ArrayList<>()).add(deviceID2Index);
  }

  @Override
  public void bufferTreeModelTablet(Tablet tablet, Boolean isAligned) {
    throw new UnsupportedOperationException(
        "PipeTableModeTsFileBuilder does not support tree model tablet to build TSFile");
  }

  @Override
  public List<Pair<String, File>> sealTsFiles() throws IOException, WriteProcessException {
    if (dataBase2TabletList.isEmpty()) {
      return new ArrayList<>(0);
    }
    List<Pair<String, File>> pairList = new ArrayList<>();
    for (Map.Entry<String, List<Tablet>> entry : dataBase2TabletList.entrySet()) {
      pairList.addAll(
          writeTableModelTabletsToTsFiles(
              entry.getValue(), tabletDeviceIdTimeIndex.get(entry.getKey()), entry.getKey()));
    }
    return pairList;
  }

  @Override
  public boolean isEmpty() {
    return dataBase2TabletList.isEmpty();
  }

  @Override
  public synchronized void onSuccess() {
    super.onSuccess();
    dataBase2TabletList.clear();
    tabletDeviceIdTimeIndex.clear();
  }

  @Override
  public synchronized void close() {
    super.close();
    dataBase2TabletList.clear();
    tabletDeviceIdTimeIndex.clear();
  }

  private List<Pair<String, File>> writeTableModelTabletsToTsFiles(
      List<Tablet> tabletList,
      List<List<Pair<IDeviceID, Integer>>> deviceIDPairMap,
      String dataBase)
      throws IOException {

    final Map<String, List<Pair<Tablet, List<Pair<IDeviceID, Integer>>>>> tableName2Tablets =
        new HashMap<>();

    // Sort the tablets by dataBaseName
    for (int i = 0; i < tabletList.size(); i++) {
      final Tablet tablet = tabletList.get(i);
      tableName2Tablets
          .computeIfAbsent(tablet.getTableName(), k -> new ArrayList<>())
          .add(new Pair<>(tablet, deviceIDPairMap.get(i)));
    }

    for (final List<Pair<Tablet, List<Pair<IDeviceID, Integer>>>> tablets :
        tableName2Tablets.values()) {
      // Let's make an assumption that as long as the tablets with non-duplicate timestamps are
      // sorted from small to large, assuming that there are N tablets that overlap with other
      // tablets, then (n+1) TSFiles must be generated. Therefore, the Builder only needs to ensure
      // that the tablets with non-overlapping times are sorted in order.
      // A[            (c,3),(d,2)],        device a [A],[B,C,D],
      // B[(a,2),(b,1),(c,4),     ],------->device b [D,A],[B,C],-------> [A->B->C],[D]
      // C[(a,3),(b,2),      (d,4)],        device c [C],[D,A,B],
      // D[(a,4),      (c,2),(d,1)]         device d [B],[D,A,C],
      tablets.sort(this::comparePairs);
    }

    // Sort the devices by tableName
    final List<String> tables = new ArrayList<>(tableName2Tablets.keySet());
    tables.sort(Comparator.naturalOrder());

    // Replace ArrayList with LinkedList to improve performance
    final LinkedHashMap<String, LinkedList<Pair<Tablet, List<Pair<IDeviceID, Integer>>>>>
        table2TabletsLinkedList = new LinkedHashMap<>();
    for (final String tableName : tables) {
      table2TabletsLinkedList.put(tableName, new LinkedList<>(tableName2Tablets.get(tableName)));
    }

    // Help GC
    tables.clear();
    tableName2Tablets.clear();

    final List<Pair<String, File>> sealedFiles = new ArrayList<>();

    // Try making the tsfile size as large as possible
    while (!table2TabletsLinkedList.isEmpty()) {
      if (Objects.isNull(fileWriter)) {
        createFileWriter();
      }

      try {
        tryBestToWriteTabletsIntoOneFile(table2TabletsLinkedList);
      } catch (final Exception e) {
        LOGGER.warn(
            "Batch id = {}: Failed to write tablets into tsfile, because {}",
            currentBatchId.get(),
            e.getMessage(),
            e);

        try {
          fileWriter.close();
        } catch (final Exception closeException) {
          LOGGER.warn(
              "Batch id = {}: Failed to close the tsfile {} after failed to write tablets into, because {}",
              currentBatchId.get(),
              fileWriter.getIOWriter().getFile().getPath(),
              closeException.getMessage(),
              closeException);
        } finally {
          // Add current writing file to the list and delete the file
          sealedFiles.add(new Pair<>(dataBase, fileWriter.getIOWriter().getFile()));
        }

        for (final Pair<String, File> sealedFile : sealedFiles) {
          final boolean deleteSuccess = FileUtils.deleteQuietly(sealedFile.right);
          LOGGER.warn(
              "Batch id = {}: {} delete the tsfile {} after failed to write tablets into {}. {}",
              currentBatchId.get(),
              deleteSuccess ? "Successfully" : "Failed to",
              sealedFile.right.getPath(),
              fileWriter.getIOWriter().getFile().getPath(),
              deleteSuccess ? "" : "Maybe the tsfile needs to be deleted manually.");
        }
        sealedFiles.clear();

        fileWriter = null;

        throw e;
      }

      fileWriter.close();
      final File sealedFile = fileWriter.getIOWriter().getFile();
      if (LOGGER.isDebugEnabled()) {
        LOGGER.debug(
            "Batch id = {}: Seal tsfile {} successfully.",
            currentBatchId.get(),
            sealedFile.getPath());
      }
      sealedFiles.add(new Pair<>(dataBase, sealedFile));
      fileWriter = null;
    }

    return sealedFiles;
  }

  private void tryBestToWriteTabletsIntoOneFile(
      final LinkedHashMap<String, LinkedList<Pair<Tablet, List<Pair<IDeviceID, Integer>>>>>
          device2TabletsLinkedList)
      throws IOException {
    final Iterator<Map.Entry<String, LinkedList<Pair<Tablet, List<Pair<IDeviceID, Integer>>>>>>
        iterator = device2TabletsLinkedList.entrySet().iterator();

    while (iterator.hasNext()) {
      final Map.Entry<String, LinkedList<Pair<Tablet, List<Pair<IDeviceID, Integer>>>>> entry =
          iterator.next();
      final String tableName = entry.getKey();
      final LinkedList<Pair<Tablet, List<Pair<IDeviceID, Integer>>>> tablets = entry.getValue();

      final List<Tablet> tabletsToWrite = new ArrayList<>();

      Pair<Tablet, List<Pair<IDeviceID, Integer>>> lastTablet = null;
      Map<IDeviceID, Long> deviceLastTimestampMap = new HashMap<>();
      while (!tablets.isEmpty()) {
        final Pair<Tablet, List<Pair<IDeviceID, Integer>>> pair = tablets.peekFirst();
        if (Objects.isNull(lastTablet) || hasNoTimestampOverlaps(pair, deviceLastTimestampMap)) {
          tabletsToWrite.add(pair.left);
          lastTablet = pair;
          tablets.pollFirst();
        } else {
          break;
        }
      }

      if (tablets.isEmpty()) {
        iterator.remove();
      }
      boolean schemaNotRegistered = true;
      for (final Tablet tablet : tabletsToWrite) {
        if (schemaNotRegistered) {
          fileWriter.registerTableSchema(
              new TableSchema(tableName, tablet.getSchemas(), tablet.getColumnTypes()));
          schemaNotRegistered = false;
        }
        try {
          fileWriter.writeTable(tablet);
        } catch (WriteProcessException e) {
          throw new PipeException(
              "The Schema in the Tablet is inconsistent with the registered TableSchema");
        }
      }
    }
  }

  private boolean hasNoTimestampOverlaps(
      final Pair<Tablet, List<Pair<IDeviceID, Integer>>> tabletPair,
      final Map<IDeviceID, Long> deviceLastTimestampMap) {
    int currentTimestampIndex = 0;
    for (Pair<IDeviceID, Integer> deviceTimestampIndexPair : tabletPair.right) {
      final Long lastDeviceTimestamp = deviceLastTimestampMap.get(deviceTimestampIndexPair.left);
      if (lastDeviceTimestamp != null
          && lastDeviceTimestamp >= tabletPair.left.timestamps[currentTimestampIndex]) {
        return false;
      }
      currentTimestampIndex = deviceTimestampIndexPair.right;
      deviceLastTimestampMap.put(
          deviceTimestampIndexPair.left, tabletPair.left.timestamps[currentTimestampIndex - 1]);
    }

    return true;
  }

  private int comparePairs(
      final Pair<Tablet, List<Pair<IDeviceID, Integer>>> pairA,
      final Pair<Tablet, List<Pair<IDeviceID, Integer>>> pairB) {
    int aCount = 0;
    int bCount = 0;
    int aIndex = 0;
    int bIndex = 0;
    int aLastTimeIndex = 0;
    int bLastTimeIndex = 0;
    final List<Pair<IDeviceID, Integer>> listA = pairA.right;
    final List<Pair<IDeviceID, Integer>> listB = pairB.right;
    while (aIndex < listA.size() && bIndex < listB.size()) {
      int comparisonResult = listA.get(aIndex).left.compareTo(listB.get(bIndex).left);
      if (comparisonResult == 0) {
        long aTime = pairA.left.timestamps[aLastTimeIndex];
        long bTime = pairB.left.timestamps[bLastTimeIndex];
        if (aTime > bTime) {
          aCount++;
        } else if (aTime < bTime) {
          bCount++;
        }
        aLastTimeIndex = listA.get(aIndex).right;
        bLastTimeIndex = listB.get(bIndex).right;
        aIndex++;
        bIndex++;
        continue;
      }

      if (comparisonResult > 0) {
        bLastTimeIndex = listB.get(bIndex).right;
        bIndex++;
        aCount++;
        continue;
      }

      aLastTimeIndex = listA.get(aIndex).right;
      aIndex++;
      bCount++;
    }
    bCount += listB.size() - bIndex;
    aCount += listA.size() - aIndex;
    return Integer.compare(bCount, aCount);
  }
}
