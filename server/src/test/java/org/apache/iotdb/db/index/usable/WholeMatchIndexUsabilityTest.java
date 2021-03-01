package org.apache.iotdb.db.index.usable;

import org.apache.iotdb.db.exception.metadata.IllegalPathException;
import org.apache.iotdb.db.metadata.PartialPath;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Set;

public class WholeMatchIndexUsabilityTest {

  @Test
  public void testMinusUsableRange() throws IllegalPathException, IOException {
    WholeMatchIndexUsability usability = new WholeMatchIndexUsability();
    // do nothing for addUsableRange
    usability.addUsableRange(new PartialPath("root.sg.d.s10"), 1, 2);
    usability.addUsableRange(new PartialPath("root.sg.d.s11"), 1, 2);

    usability.minusUsableRange(new PartialPath("root.sg.d.s1"), 1, 2);
    usability.minusUsableRange(new PartialPath("root.sg.d.s2"), 1, 2);
    usability.minusUsableRange(new PartialPath("root.sg.d.s3"), 1, 2);
    Set<PartialPath> ret = usability.getUnusableRange();
    Assert.assertEquals("[root.sg.d.s3, root.sg.d.s2, root.sg.d.s1]", ret.toString());
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    usability.serialize(out);
    InputStream in = new ByteArrayInputStream(out.toByteArray());
    WholeMatchIndexUsability usable2 = new WholeMatchIndexUsability();
    usable2.deserialize(in);
    Set<PartialPath> ret2 = usability.getUnusableRange();
    Assert.assertEquals("[root.sg.d.s3, root.sg.d.s2, root.sg.d.s1]", ret2.toString());
  }
}
