package org.apache.iotdb.commons.udf;

import org.apache.iotdb.common.rpc.thrift.FunctionType;
import org.apache.iotdb.common.rpc.thrift.Model;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;

public class UDFTypeTest {

  @Test
  public void testSerializationAndDeserialization() throws IOException {
    // Testing serialization and deserialization for each UDFType
    for (UDFType udfType : UDFType.values()) {
      // Serialize the UDFType into a byte array
      ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
      DataOutputStream dataOutputStream = new DataOutputStream(byteArrayOutputStream);
      udfType.serialize(dataOutputStream);

      // Convert the byte array into a ByteBuffer for deserialization
      ByteBuffer byteBuffer = ByteBuffer.wrap(byteArrayOutputStream.toByteArray());
      UDFType deserializedUdfType = UDFType.deserialize(byteBuffer);

      // Assert that the deserialized UDFType matches the original
      Assert.assertEquals(udfType, deserializedUdfType);
    }
  }

  @Test
  public void testPublicInterfaces() {
    List<UDFType> tree =
        Arrays.asList(
            UDFType.of(Model.TREE, FunctionType.NULL, true),
            UDFType.of(Model.TREE, FunctionType.NULL, false));
    List<UDFType> table =
        Arrays.asList(
            UDFType.of(Model.TABLE, FunctionType.SCALAR, true),
            UDFType.of(Model.TABLE, FunctionType.SCALAR, false),
            UDFType.of(Model.TABLE, FunctionType.AGGREGATE, true),
            UDFType.of(Model.TABLE, FunctionType.AGGREGATE, false),
            UDFType.of(Model.TABLE, FunctionType.TABLE, true),
            UDFType.of(Model.TABLE, FunctionType.TABLE, false));
    // Testing public methods for all UDFType values
    for (UDFType udfType : tree) {
      Assert.assertTrue(udfType.isTreeModel());
      Assert.assertFalse(udfType.isTableModel());
      Assert.assertEquals(FunctionType.NULL, udfType.getType());
    }
    for (UDFType udfType : table) {
      Assert.assertFalse(udfType.isTreeModel());
      Assert.assertTrue(udfType.isTableModel());
      Assert.assertNotEquals(FunctionType.NULL, udfType.getType());
    }
  }
}
