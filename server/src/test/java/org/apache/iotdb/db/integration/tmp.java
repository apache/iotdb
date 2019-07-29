package org.apache.iotdb.db.integration;

import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class tmp {

  public static void main(String[] args) {
    System.out.println(hashMod(String.format("%d%s", 1, "MLH8O7T*Y8HO*&"), 5));
//    System.out.println(hashMod(String.format("%d%s", 1,"MLH8O7T*Y8HO*&"), markRate));
//    System.out.println(hashMod(String.format("%s%d%s", secretKey, timestamp, secretKey), groupNumber));
  }

  private static int hashMod(String val, Integer base) {
    MessageDigest md;
    try {
      md = MessageDigest.getInstance("MD5");
    } catch (NoSuchAlgorithmException e) {
      e.printStackTrace();
      throw new RuntimeException("ERROR: Cannot find MD5 algorithm!");
    }
    md.update(val.getBytes());
    BigInteger resultInteger = new BigInteger(1, md.digest());
    return resultInteger.mod(new BigInteger(base.toString())).intValue();
  }
}
