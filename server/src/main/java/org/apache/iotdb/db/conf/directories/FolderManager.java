package org.apache.iotdb.db.conf.directories;

import org.apache.iotdb.db.conf.directories.strategy.DirectoryStrategy;
import org.apache.iotdb.db.conf.directories.strategy.DirectoryStrategyType;
import org.apache.iotdb.db.conf.directories.strategy.MaxDiskUsableSpaceFirstStrategy;
import org.apache.iotdb.db.conf.directories.strategy.MinFolderOccupiedSpaceFirstStrategy;
import org.apache.iotdb.db.conf.directories.strategy.RandomOnDiskUsableSpaceStrategy;
import org.apache.iotdb.db.conf.directories.strategy.SequenceStrategy;
import org.apache.iotdb.db.exception.DiskSpaceInsufficientException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class FolderManager {
  private static final Logger logger = LoggerFactory.getLogger(FolderManager.class);

  private final List<String> folders;
  private final DirectoryStrategy selectStrategy;

  public FolderManager(List<String> folders, DirectoryStrategyType type)
      throws DiskSpaceInsufficientException {
    this.folders = folders;
    switch (type) {
      case SEQUENCE_STRATEGY:
        this.selectStrategy = new SequenceStrategy();
        break;
      case MAX_DISK_USABLE_SPACE_FIRST_STRATEGY:
        this.selectStrategy = new MaxDiskUsableSpaceFirstStrategy();
        break;
      case MIN_FOLDER_OCCUPIED_SPACE_FIRST_STRATEGY:
        this.selectStrategy = new MinFolderOccupiedSpaceFirstStrategy();
        break;
      case RANDOM_ON_DISK_USABLE_SPACE_STRATEGY:
        this.selectStrategy = new RandomOnDiskUsableSpaceStrategy();
        break;
      default:
        throw new RuntimeException();
    }
    this.selectStrategy.setFolders(folders);
  }

  public String getNextFolder() throws DiskSpaceInsufficientException {
    return folders.get(selectStrategy.nextFolderIndex());
  }
}
