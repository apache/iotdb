package org.apache.iotdb.cluster.log;

import org.apache.iotdb.cluster.exception.EntryStabledException;
import org.apache.iotdb.cluster.exception.EntryUnavailableException;
import org.apache.iotdb.cluster.exception.GetEntriesWrongParametersException;
import org.apache.iotdb.cluster.exception.TruncateCommittedEntryException;
import org.apache.iotdb.cluster.log.logtypes.PhysicalPlanLog;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class UnCommittedEntryManagerTest {

  static class UnCommitEntryManagerTesterBase {
    public List<Log> entries;
    public long offset;

    public UnCommitEntryManagerTesterBase(List<Log> entries, long offset) {
      this.entries = entries;
      this.offset = offset;
    }
  }

  @Test
  public void getFirstUnCommittedIndex() {
    class UnCommitEntryManagerTester extends UnCommitEntryManagerTesterBase {
      public long testOffset;

      public UnCommitEntryManagerTester(List<Log> entries, long offset, long testOffset) {
        super(entries, offset);
        this.testOffset = testOffset;
      }
    }
    List<UnCommitEntryManagerTester> tests = new ArrayList<UnCommitEntryManagerTester>() {{
      add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
      }}, 0, 0));
      add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
      }}, 5, 5));
      add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(6, 1));
      }}, 5, 5));
    }};
    for (UnCommitEntryManagerTester test : tests) {
      UnCommittedEntryManager instance = new UnCommittedEntryManager(test.offset, test.entries);
      long index = instance.getFirstUnCommittedIndex();
      assertEquals(test.testOffset, index);
    }
  }

  @Test
  public void maybeLastIndex() {
    class UnCommitEntryManagerTester extends UnCommitEntryManagerTesterBase {
      public long testIndex;

      public UnCommitEntryManagerTester(List<Log> entries, long offset, long testIndex) {
        super(entries, offset);
        this.testIndex = testIndex;
      }
    }
    List<UnCommitEntryManagerTester> tests = new ArrayList<UnCommitEntryManagerTester>() {{
      add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
      }}, 0, -1));
      add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
      }}, 5, 5));
      add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
        add(new PhysicalPlanLog(6, 1));
      }}, 5, 6));
    }};
    for (UnCommitEntryManagerTester test : tests) {
      UnCommittedEntryManager instance = new UnCommittedEntryManager(test.offset, test.entries);
      long index = instance.maybeLastIndex();
      assertEquals(test.testIndex, index);
    }
  }

  @Test
  public void maybeTerm() {
    class UnCommitEntryManagerTester extends UnCommitEntryManagerTesterBase {
      public long index;
      public long testTerm;

      public UnCommitEntryManagerTester(List<Log> entries, long offset, long index, long testTerm) {
        super(entries, offset);
        this.index = index;
        this.testTerm = testTerm;
      }
    }
    List<UnCommitEntryManagerTester> tests = new ArrayList<UnCommitEntryManagerTester>() {{
      add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
      }}, 0, 0, -1));
      add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
      }}, 5, 5, 1));
      add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
      }}, 5, 6, -1));
      add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
      }}, 5, 4, -1));
      add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
        add(new PhysicalPlanLog(6, 4));
      }}, 5, 5, 1));
      add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
        add(new PhysicalPlanLog(6, 4));
      }}, 5, 6, 4));
    }};
    for (UnCommitEntryManagerTester test : tests) {
      UnCommittedEntryManager instance = new UnCommittedEntryManager(test.offset, test.entries);
      long term = instance.maybeTerm(test.index);
      assertEquals(test.testTerm, term);
    }
  }

  @Test
  public void stableTo() {
    class UnCommitEntryManagerTester extends UnCommitEntryManagerTesterBase {
      public long index;
      public long term;
      public long testOffset;
      public long testLen;

      public UnCommitEntryManagerTester(List<Log> entries, long offset, long index, long term, long testOffset, long testLen) {
        super(entries, offset);
        this.index = index;
        this.term = term;
        this.testOffset = testOffset;
        this.testLen = testLen;
      }
    }
    List<UnCommitEntryManagerTester> tests = new ArrayList<UnCommitEntryManagerTester>() {{
      // empty entries
      add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
      }}, 0, 5, 1, 0, 0));
      // stable to the first entry
      add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
      }}, 5, 5, 1, 6, 0));
      // stable to the first entry
      add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
        add(new PhysicalPlanLog(6, 1));
      }}, 5, 5, 1, 6, 1));
      // stable to the first entry and term mismatch
      add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(6, 2));
      }}, 6, 6, 1, 6, 1));
      // stable to old entry
      add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
      }}, 5, 4, 1, 5, 1));
      // stable to old entry
      add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
      }}, 5, 2, 2, 5, 1));
    }};
    for (UnCommitEntryManagerTester test : tests) {
      UnCommittedEntryManager instance = new UnCommittedEntryManager(test.offset, test.entries);
      instance.stableTo(test.index, test.term);
      assertEquals(test.testOffset, instance.getFirstUnCommittedIndex());
      assertEquals(test.testLen, instance.getEntries().size());
    }
  }

  @Test
  public void truncateAndAppendNormal() {
    class UnCommitEntryManagerTester extends UnCommitEntryManagerTesterBase {
      public List<Log> toAppend;
      public long testOffset;
      public List<Log> testEntries;

      public UnCommitEntryManagerTester(List<Log> entries, long offset, List<Log> toAppend, long testOffset, List<Log> testEntries) {
        super(entries, offset);
        this.toAppend = toAppend;
        this.testOffset = testOffset;
        this.testEntries = testEntries;
      }
    }
    List<UnCommitEntryManagerTester> tests = new ArrayList<UnCommitEntryManagerTester>() {{
      // append to the end
      add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
      }}, 5,
              new ArrayList<Log>() {{
                add(new PhysicalPlanLog(6, 1));
                add(new PhysicalPlanLog(7, 1));
              }}
              , 5,
              new ArrayList<Log>() {{
                add(new PhysicalPlanLog(5, 1));
                add(new PhysicalPlanLog(6, 1));
                add(new PhysicalPlanLog(7, 1));
              }}));
      // replace the uncommitted entries
      add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
      }}, 5,
              new ArrayList<Log>() {{
                add(new PhysicalPlanLog(5, 2));
                add(new PhysicalPlanLog(6, 2));
              }}
              , 5,
              new ArrayList<Log>() {{
                add(new PhysicalPlanLog(5, 2));
                add(new PhysicalPlanLog(6, 2));
              }}));
      // truncate the existing entries and append
      add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
        add(new PhysicalPlanLog(6, 1));
        add(new PhysicalPlanLog(7, 1));
      }}, 5,
              new ArrayList<Log>() {{
                add(new PhysicalPlanLog(6, 2));
              }}
              , 5,
              new ArrayList<Log>() {{
                add(new PhysicalPlanLog(5, 1));
                add(new PhysicalPlanLog(6, 2));
              }}));
      // truncate the existing entries and append
      add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
        add(new PhysicalPlanLog(5, 1));
        add(new PhysicalPlanLog(6, 1));
        add(new PhysicalPlanLog(7, 1));
      }}, 5,
              new ArrayList<Log>() {{
                add(new PhysicalPlanLog(7, 2));
                add(new PhysicalPlanLog(8, 2));
              }}
              , 5,
              new ArrayList<Log>() {{
                add(new PhysicalPlanLog(5, 1));
                add(new PhysicalPlanLog(6, 1));
                add(new PhysicalPlanLog(7, 2));
                add(new PhysicalPlanLog(8, 2));
              }}));
    }};
    for (UnCommitEntryManagerTester test : tests) {
      UnCommittedEntryManager instance = new UnCommittedEntryManager(test.offset, test.entries);
      try {
        instance.truncateAndAppend(test.toAppend);
      } catch (TruncateCommittedEntryException e) {
        fail("An unexpected exception was thrown.");
      }
      assertEquals(test.testOffset, instance.getFirstUnCommittedIndex());
      assertEquals(test.testEntries, instance.getEntries());
    }
  }

  @Test
  public void truncateAndAppendUnNormal() {
    class UnCommitEntryManagerTester extends UnCommitEntryManagerTesterBase {
      public List<Log> toAppend;

      public UnCommitEntryManagerTester(List<Log> entries, long offset, List<Log> toAppend) {
        super(entries, offset);
        this.toAppend = toAppend;
      }
    }
    List<UnCommitEntryManagerTester> tests = new ArrayList<UnCommitEntryManagerTester>() {
      {
        // append to the end
        add(new UnCommitEntryManagerTester(new ArrayList<Log>() {{
          add(new PhysicalPlanLog(5, 1));
        }}, 5,
                new ArrayList<Log>() {
                  {
                    add(new PhysicalPlanLog(4, 2));
                    add(new PhysicalPlanLog(5, 2));
                  }
                }));
      }
    };
    for (UnCommitEntryManagerTester test : tests) {
      UnCommittedEntryManager instance = new UnCommittedEntryManager(test.offset, test.entries);
      try {
        instance.truncateAndAppend(test.toAppend);
        fail("The expected exception is not thrown");
      } catch (TruncateCommittedEntryException e) {
      }
    }
  }

  @Test
  public void getEntriesNormal() {
    class UnCommitEntryManagerTester {
      public long from;
      public long to;
      public List<Log> testEntries;

      public UnCommitEntryManagerTester(long from, long to, List<Log> testEntries) {
        this.from = from;
        this.to = to;
        this.testEntries = testEntries;
      }
    }
    long offset = 100;
    long num = 100;
    List<Log> entries = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      entries.add(new PhysicalPlanLog(offset + i, offset + i));
    }
    UnCommittedEntryManager instance = new UnCommittedEntryManager(offset, entries);
    List<UnCommitEntryManagerTester> tests = new ArrayList<UnCommitEntryManagerTester>() {{
      add(new UnCommitEntryManagerTester(offset, offset + num, entries));
      add(new UnCommitEntryManagerTester(offset, offset + 1, new ArrayList<Log>() {{
        add(new PhysicalPlanLog(offset, offset));
      }}));
      add(new UnCommitEntryManagerTester(offset + num - 1, offset + num, new ArrayList<Log>() {{
        add(new PhysicalPlanLog(offset + num - 1, offset + num - 1));
      }}));
    }};
    for (UnCommitEntryManagerTester test : tests) {
      List<Log> answer = null;
      try {
        answer = instance.getEntries(test.from, test.to);
      } catch (Exception e) {
        fail("An unexpected exception was thrown.");
      }
      assertEquals(answer, test.testEntries);
    }
  }

  @Test
  public void getEntriesUnNormal() {
    class UnCommitEntryManagerTester {
      public long from;
      public long to;

      public UnCommitEntryManagerTester(long from, long to) {
        this.from = from;
        this.to = to;
      }
    }
    long offset = 100;
    long num = 100;
    List<Log> entries = new ArrayList<>();
    for (int i = 0; i < num; i++) {
      entries.add(new PhysicalPlanLog(offset + i, offset + i));
    }
    UnCommittedEntryManager instance = new UnCommittedEntryManager(offset, entries);
    List<UnCommitEntryManagerTester> tests = new ArrayList<UnCommitEntryManagerTester>() {{
      add(new UnCommitEntryManagerTester(offset, offset));
      add(new UnCommitEntryManagerTester(offset + 1, offset));
      add(new UnCommitEntryManagerTester(offset - 1, offset));
      add(new UnCommitEntryManagerTester(offset - 1, offset + 1));
      add(new UnCommitEntryManagerTester(offset + num - 1, offset + num + 1));
      add(new UnCommitEntryManagerTester(offset + num, offset + num + 1));
    }};
    for (UnCommitEntryManagerTester test : tests) {
      try {
        instance.getEntries(test.from, test.to);
        fail("The expected exception is not thrown");
      } catch (Exception e) {
        if(!((e instanceof GetEntriesWrongParametersException && test.from >= test.to)||
                (e instanceof EntryStabledException && test.from < offset)||
                (e instanceof EntryUnavailableException && test.from >= offset && test.to > offset + num))){
          fail("The expected exception is not thrown");
        }
      }
    }
  }
}