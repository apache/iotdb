package org.apache.iotdb.db.conf.directories.strategy;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;
import org.apache.iotdb.db.utils.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest(FileUtils.class)
public class DirectoryStrategyTest {

  List<String> dataDirList;
  Set<Integer> fullDirIndexSet;

  @Before
  public void setUp() throws DiskSpaceInsufficientException, IOException {
    dataDirList = new ArrayList<>();
    for (int i = 0; i < 5; i++) {
      dataDirList.add("data" + i);
    }

    fullDirIndexSet = new HashSet<>();
    fullDirIndexSet.add(1);
    fullDirIndexSet.add(3);

    PowerMockito.mockStatic(FileUtils.class);
    for (int i = 0; i < dataDirList.size(); i++) {
      boolean res = !fullDirIndexSet.contains(i);
      PowerMockito.when(FileUtils.hasSpace(dataDirList.get(i))).thenReturn(res);
      PowerMockito.when(FileUtils.getUsableSpace(dataDirList.get(i))).thenReturn(res ? (long) (i + 1) : 0L);
      PowerMockito.when(FileUtils.getOccupiedSpace(dataDirList.get(i))).thenReturn(res ? (long) (i + 1) : Long.MAX_VALUE);
    }
  }

  @After
  public void tearDown() {

  }

  @Test
  public void testSequenceStrategy() throws DiskSpaceInsufficientException {
    SequenceStrategy sequenceStrategy = new SequenceStrategy();
    sequenceStrategy.init(dataDirList);

    // loop two times of data dir size to fully loop
    int index = 0;
    for (int i = 0; i < dataDirList.size() * 2; i++, index++) {
      index = index % dataDirList.size();
      while (fullDirIndexSet.contains(index)) {
        index = (index + 1) % dataDirList.size();
      }
      assertEquals(index, sequenceStrategy.nextFolderIndex());
    }
  }

  @Test
  public void testMaxDiskUsableSpaceFirstStrategy() throws DiskSpaceInsufficientException {
    MaxDiskUsableSpaceFirstStrategy maxDiskUsableSpaceFirstStrategy = new MaxDiskUsableSpaceFirstStrategy();
    maxDiskUsableSpaceFirstStrategy.init(dataDirList);

    int maxIndex = getIndexOfMaxSpace();
    for (int i = 0; i < dataDirList.size(); i++) {
      assertEquals(maxIndex, maxDiskUsableSpaceFirstStrategy.nextFolderIndex());
    }

    PowerMockito.when(FileUtils.getUsableSpace(dataDirList.get(maxIndex))).thenReturn(0L);
    maxIndex = getIndexOfMaxSpace();
    for (int i = 0; i < dataDirList.size(); i++) {
      assertEquals(maxIndex, maxDiskUsableSpaceFirstStrategy.nextFolderIndex());
    }
  }

  private int getIndexOfMaxSpace() {
    int index = -1;
    long maxSpace = -1;
    for (int i = 0; i < dataDirList.size(); i++) {
      long space = FileUtils.getUsableSpace(dataDirList.get(i));
      if (maxSpace < space) {
        index = i;
        maxSpace = space;
      }
    }
    return index;
  }

  @Test
  public void testMinFolderOccupiedSpaceFirstStrategy()
      throws DiskSpaceInsufficientException, IOException {
    MinFolderOccupiedSpaceFirstStrategy minFolderOccupiedSpaceFirstStrategy = new MinFolderOccupiedSpaceFirstStrategy();
    minFolderOccupiedSpaceFirstStrategy.init(dataDirList);

    int minIndex = getIndexOfMinOccupiedSpace();
    for (int i = 0; i < dataDirList.size(); i++) {
      assertEquals(minIndex, minFolderOccupiedSpaceFirstStrategy.nextFolderIndex());
    }

    PowerMockito.when(FileUtils.getOccupiedSpace(dataDirList.get(minIndex))).thenReturn(Long.MAX_VALUE);
    minIndex = getIndexOfMinOccupiedSpace();
    for (int i = 0; i < dataDirList.size(); i++) {
      assertEquals(minIndex, minFolderOccupiedSpaceFirstStrategy.nextFolderIndex());
    }
  }

  private int getIndexOfMinOccupiedSpace() throws IOException {
    int index = -1;
    long minOccupied = Long.MAX_VALUE;
    for (int i = 0; i < dataDirList.size(); i++) {
      long space = FileUtils.getOccupiedSpace(dataDirList.get(i));
      if (minOccupied > space) {
        index = i;
        minOccupied = space;
      }
    }
    return index;
  }

  @Test
  public void testAllDiskFull() {
    for (int i = 0; i < dataDirList.size(); i++) {
      PowerMockito.when(FileUtils.hasSpace(dataDirList.get(i))).thenReturn(false);
    }

    SequenceStrategy sequenceStrategy = new SequenceStrategy();
    try {
      sequenceStrategy.init(dataDirList);
      fail();
    } catch (DiskSpaceInsufficientException e) {
    }

    MaxDiskUsableSpaceFirstStrategy maxDiskUsableSpaceFirstStrategy = new MaxDiskUsableSpaceFirstStrategy();
    try {
      maxDiskUsableSpaceFirstStrategy.init(dataDirList);
      fail();
    } catch (DiskSpaceInsufficientException e) {
    }

    MinFolderOccupiedSpaceFirstStrategy minFolderOccupiedSpaceFirstStrategy = new MinFolderOccupiedSpaceFirstStrategy();
    try {
      minFolderOccupiedSpaceFirstStrategy.init(dataDirList);
      fail();
    } catch (DiskSpaceInsufficientException e) {
    }
  }
}