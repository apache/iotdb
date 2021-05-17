package org.apache.iotdb.tsfile.encoding.decoder;

import org.apache.iotdb.tsfile.encoding.encoder.DictionaryEncoder;
import org.apache.iotdb.tsfile.utils.Binary;

import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DictionaryDecoderTest {
  private DictionaryEncoder encoder = new DictionaryEncoder();
  private DictionaryDecoder decoder = new DictionaryDecoder();
  private ByteArrayOutputStream baos = new ByteArrayOutputStream();

  @Test
  public void testSingle() {
    testAll("a");
    testAll("b");
    testAll("c");
  }

  @Test
  public void testAllUnique() {
      testAll("a", "b", "c");
      testAll("x", "o", "q");
      testAll(",", ".", "c", "b", "e");
  }

  @Test
  public void testAllSame() {
      testAll("a", "a", "a");
      testAll("b", "b", "b");
  }

  private void testAll(String... all) {
    for (String s : all) {
        encoder.encode(new Binary(s), baos);
    }
    encoder.flush(baos);

    ByteBuffer out = ByteBuffer.wrap(baos.toByteArray());

    for (String s : all) {
        assertTrue(decoder.hasNext(out));
        assertEquals(s, decoder.readBinary(out).getStringValue());
    }


    decoder.reset();
    baos.reset();
  }
}
