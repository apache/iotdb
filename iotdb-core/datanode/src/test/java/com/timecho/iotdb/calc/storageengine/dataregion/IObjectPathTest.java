package com.timecho.iotdb.calc.storageengine.dataregion;

import org.apache.tsfile.file.metadata.StringArrayDeviceID;
import org.junit.Assert;
import org.junit.Test;

import java.nio.ByteBuffer;

public class IObjectPathTest {

  @Test
  public void testPlainObjectPathSerde1() {
    PlainObjectPath objectPath = new PlainObjectPath(1, 0, new StringArrayDeviceID("t1.d1"), "s1");
    ByteBuffer buffer = ByteBuffer.allocate(objectPath.getSerializedSize());
    objectPath.serialize(buffer);
    buffer.flip();
    PlainObjectPath deserialized = PlainObjectPath.deserialize(buffer);
    Assert.assertEquals(objectPath.getTime(), deserialized.getTime());
    Assert.assertEquals(objectPath.getMeasurement(), deserialized.getMeasurement());
    Assert.assertEquals(objectPath.getPath(), deserialized.getPath());
    Assert.assertEquals(objectPath.getDeviceID(), deserialized.getDeviceID());
  }

  @Test
  public void testPlainObjectPathSerde2() {
    PlainObjectPath objectPath =
        new PlainObjectPath(1, 0, new StringArrayDeviceID("t1", null, "d2"), "s1");
    ByteBuffer buffer = ByteBuffer.allocate(objectPath.getSerializedSize());
    objectPath.serialize(buffer);
    buffer.flip();
    PlainObjectPath deserialized = PlainObjectPath.deserialize(buffer);
    Assert.assertEquals(objectPath.getTime(), deserialized.getTime());
    Assert.assertEquals(objectPath.getMeasurement(), deserialized.getMeasurement());
    Assert.assertEquals(objectPath.getPath(), deserialized.getPath());
    Assert.assertEquals(objectPath.getDeviceID(), deserialized.getDeviceID());
  }

  @Test
  public void testPlainObjectPathSerde3() {
    PlainObjectPath objectPath =
        new PlainObjectPath(1, 0, new StringArrayDeviceID("t1", "", "d3"), "s1");
    ByteBuffer buffer = ByteBuffer.allocate(objectPath.getSerializedSize());
    objectPath.serialize(buffer);
    buffer.flip();
    PlainObjectPath deserialized = PlainObjectPath.deserialize(buffer);
    Assert.assertEquals(objectPath.getTime(), deserialized.getTime());
    Assert.assertEquals(objectPath.getMeasurement(), deserialized.getMeasurement());
    Assert.assertEquals(objectPath.getPath(), deserialized.getPath());
    Assert.assertEquals(objectPath.getDeviceID(), deserialized.getDeviceID());
  }

  @Test
  public void testBase32ObjectPathSerde1() {
    Base32ObjectPath objectPath =
        new Base32ObjectPath(1, 0, new StringArrayDeviceID("t1.d1"), "s1");
    ByteBuffer buffer = ByteBuffer.allocate(objectPath.getSerializedSize());
    objectPath.serialize(buffer);
    buffer.flip();
    Base32ObjectPath deserialized = Base32ObjectPath.deserialize(buffer);
    Assert.assertEquals(objectPath.getTime(), deserialized.getTime());
    Assert.assertEquals(objectPath.getMeasurement(), deserialized.getMeasurement());
    Assert.assertEquals(objectPath.getPath(), deserialized.getPath());
    Assert.assertEquals(objectPath.getDeviceID(), deserialized.getDeviceID());
  }

  @Test
  public void testBase32ObjectPathSerde2() {
    Base32ObjectPath objectPath =
        new Base32ObjectPath(1, 0, new StringArrayDeviceID("t1", null, "d2"), "s1");
    ByteBuffer buffer = ByteBuffer.allocate(objectPath.getSerializedSize());
    objectPath.serialize(buffer);
    buffer.flip();
    Base32ObjectPath deserialized = Base32ObjectPath.deserialize(buffer);
    Assert.assertEquals(objectPath.getTime(), deserialized.getTime());
    Assert.assertEquals(objectPath.getMeasurement(), deserialized.getMeasurement());
    Assert.assertEquals(objectPath.getPath(), deserialized.getPath());
    Assert.assertEquals(objectPath.getDeviceID(), deserialized.getDeviceID());
  }

  @Test
  public void testBase32ObjectPathSerde3() {
    Base32ObjectPath objectPath =
        new Base32ObjectPath(1, 0, new StringArrayDeviceID("t1", "", "d3"), "s1");
    ByteBuffer buffer = ByteBuffer.allocate(objectPath.getSerializedSize());
    objectPath.serialize(buffer);
    buffer.flip();
    Base32ObjectPath deserialized = Base32ObjectPath.deserialize(buffer);
    Assert.assertEquals(objectPath.getTime(), deserialized.getTime());
    Assert.assertEquals(objectPath.getMeasurement(), deserialized.getMeasurement());
    Assert.assertEquals(objectPath.getPath(), deserialized.getPath());
    Assert.assertEquals(objectPath.getDeviceID(), deserialized.getDeviceID());
  }
}
