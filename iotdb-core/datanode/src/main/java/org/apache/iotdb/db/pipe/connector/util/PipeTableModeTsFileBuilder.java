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
  private final Map<String, List<Map<IDeviceID, Pair<Long, Long>>>> tabletDeviceIdTimeRange =
      new HashMap<>();

  public PipeTableModeTsFileBuilder(AtomicLong currentBatchId, AtomicLong tsFileIdGenerator) {
    super(currentBatchId, tsFileIdGenerator);
  }

  @Override
  public void bufferTableModelTablet(
      String dataBase, Tablet tablet, Map<IDeviceID, Pair<Long, Long>> deviceID2TimeRange) {
    dataBase2TabletList.computeIfAbsent(dataBase, db -> new ArrayList<>()).add(tablet);
    tabletDeviceIdTimeRange
        .computeIfAbsent(dataBase, db -> new ArrayList<>())
        .add(deviceID2TimeRange);
  }

  @Override
  public void bufferTreeModelTablet(Tablet tablet, Boolean isAligned) {}

  @Override
  public List<Pair<String, File>> sealTsFiles() throws IOException, WriteProcessException {
    if (dataBase2TabletList.isEmpty()) {
      return new ArrayList<>(0);
    }
    List<Pair<String, File>> pairList = new ArrayList<>();
    for (Map.Entry<String, List<Tablet>> entry : dataBase2TabletList.entrySet()) {
      pairList.addAll(
          writeTableModelTabletsToTsFiles(
              entry.getValue(), tabletDeviceIdTimeRange.get(entry.getKey()), entry.getKey()));
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
  }

  @Override
  public synchronized void close() {
    super.close();
    dataBase2TabletList.clear();
  }

  private List<Pair<String, File>> writeTableModelTabletsToTsFiles(
      List<Tablet> tabletList,
      List<Map<IDeviceID, Pair<Long, Long>>> deviceIDPairMap,
      String dataBase)
      throws IOException, WriteProcessException {

    final Map<String, List<Pair<Tablet, Map<IDeviceID, Pair<Long, Long>>>>> tableName2Tablets =
        new HashMap<>();

    // Sort the tablets by dataBaseName
    for (int i = 0; i < tabletList.size(); i++) {
      final Tablet tablet = tabletList.get(i);
      tableName2Tablets
          .computeIfAbsent(tablet.getTableName(), k -> new ArrayList<>())
          .add(new Pair<>(tablet, deviceIDPairMap.get(i)));
    }

    // Sort the tablets by start time in each device
    for (final List<Pair<Tablet, Map<IDeviceID, Pair<Long, Long>>>> tablets :
        tableName2Tablets.values()) {

      tablets.sort(
          // Each tablet has at least one timestamp
          Comparator.comparingLong(pair -> pair.left.timestamps[0]));
    }

    // Sort the devices by tableName
    final List<String> tables = new ArrayList<>(tableName2Tablets.keySet());
    tables.sort(Comparator.naturalOrder());

    // Replace ArrayList with LinkedList to improve performance
    final LinkedHashMap<String, LinkedList<Pair<Tablet, Map<IDeviceID, Pair<Long, Long>>>>>
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
      final LinkedHashMap<String, LinkedList<Pair<Tablet, Map<IDeviceID, Pair<Long, Long>>>>>
          device2TabletsLinkedList)
      throws IOException, WriteProcessException {
    final Iterator<Map.Entry<String, LinkedList<Pair<Tablet, Map<IDeviceID, Pair<Long, Long>>>>>>
        iterator = device2TabletsLinkedList.entrySet().iterator();

    while (iterator.hasNext()) {
      final Map.Entry<String, LinkedList<Pair<Tablet, Map<IDeviceID, Pair<Long, Long>>>>> entry =
          iterator.next();
      final String tableName = entry.getKey();
      final LinkedList<Pair<Tablet, Map<IDeviceID, Pair<Long, Long>>>> tablets = entry.getValue();

      final List<Tablet> tabletsToWrite = new ArrayList<>();

      Pair<Tablet, Map<IDeviceID, Pair<Long, Long>>> lastTablet = null;
      while (!tablets.isEmpty()) {
        final Pair<Tablet, Map<IDeviceID, Pair<Long, Long>>> pair = tablets.peekFirst();
        if (Objects.isNull(lastTablet)) {
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
          throw new PipeException("");
        }
      }
    }
  }

  //  private boolean isOver(Map<IDeviceID,Pair<Long,Long>> lastPair,Map<IDeviceID,Pair<Long,Long>>
  // currPair){
  //    for(Map.Entry<IDeviceID,Pair<Long,Long>> entry : lastPair.entrySet()){
  //      Pair<Long,Long> pair = currPair.get(entry.getKey());
  //      long lastPairL=
  //      if(pair.left>entry.getValue().right)
  //    }
  //  }
}
