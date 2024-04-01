package org.apache.iotdb.consensus.natraft.protocol.log.manager.serialization;

import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.natraft.protocol.RaftConfig;
import org.apache.iotdb.consensus.natraft.protocol.log.Entry;
import org.apache.iotdb.consensus.natraft.protocol.log.logtype.EmptyEntry;

import org.apache.ratis.util.FileUtils;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class SyncLogDequeSerializerTest {

  private final String storageDir = "target" + File.separator + "raftlog";

  @Before
  public void setUp() {
    new File(storageDir).mkdirs();
  }

  @After
  public void cleanUp() throws IOException {
    FileUtils.deleteFully(new File(storageDir));
  }

  @Test
  public void testRestart() throws IOException {
    ConsensusGroupId groupId = ConsensusGroupId.Factory.create(0, 0);
    RaftConfig config =
        new RaftConfig(ConsensusConfig.newBuilder().setStorageDir(storageDir).build());
    SyncLogDequeSerializer dequeSerializer = new SyncLogDequeSerializer(groupId, config, null);

    int entryNum = 100;
    List<Entry> entries = new ArrayList<>();
    for (int i = 0; i < entryNum; i++) {
      EmptyEntry emptyEntry = new EmptyEntry();
      emptyEntry.setCurrLogIndex(i);
      entries.add(emptyEntry);
    }

    dequeSerializer.append(entries, 0, 0);
    dequeSerializer.close();

    dequeSerializer = new SyncLogDequeSerializer(groupId, config, null);
    List<Entry> recoveredEntries = dequeSerializer.getAllEntriesAfterAppliedIndex();
    assertEquals(entries, recoveredEntries);
  }
}
