package org.apache.iotdb.cluster.expr.vgraft;

import org.apache.iotdb.cluster.rpc.thrift.Node;
import org.apache.iotdb.cluster.utils.ClusterUtils;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.security.InvalidKeyException;
import java.security.KeyPair;
import java.security.KeyPairGenerator;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.SecureRandom;
import java.security.Signature;
import java.security.SignatureException;
import java.util.Arrays;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class KeyManager {

  public static KeyManager INSTANCE = new KeyManager();

  private KeyManager() {}

  private KeyPairGenerator keygen;
  private SecureRandom sRandom;
  private KeyPair keyPair;
  private byte[] nodeSignature;
  private byte[] dummySignature;
  private Map<Node, byte[]> nodeSignatures = new ConcurrentHashMap<>();

  public void init(Node thisNode) {
    try {
      keygen = KeyPairGenerator.getInstance("DSA", "SUN");
      sRandom = SecureRandom.getInstance("SHA1PRNG", "SUN");
      keygen.initialize(1024, sRandom);
      keyPair = keygen.generateKeyPair();
      Signature dsa = Signature.getInstance("SHA1withDSA", "SUN");
      dsa.initSign(keyPair.getPrivate());
      dsa.update(ClusterUtils.nodeToString(thisNode).getBytes(StandardCharsets.UTF_8));
      nodeSignature = dsa.sign();
      dummySignature = ClusterUtils.nodeToString(thisNode).getBytes(StandardCharsets.UTF_8);
    } catch (NoSuchAlgorithmException
        | NoSuchProviderException
        | InvalidKeyException
        | SignatureException e) {
      throw new RuntimeException(e);
    }
  }

  public byte[] getNodeSignature() {
    return dummySignature;
  }

  public boolean verifyNodeSignature(Node node, byte[] signature, int offset, int size) {
    // simulate the verifying time
    try {
      Signature dsa;
      dsa = Signature.getInstance("SHA1withDSA", "SUN");
      dsa.initVerify(keyPair.getPublic());
      dsa.verify(nodeSignature);
    } catch (NoSuchAlgorithmException
        | NoSuchProviderException
        | SignatureException
        | InvalidKeyException e) {
      throw new RuntimeException(e);
    }

    return Arrays.equals(
        ClusterUtils.nodeToString(node).getBytes(StandardCharsets.UTF_8), signature);
  }

  public boolean verifyNodeSignature(Node node, ByteBuffer buffer) {
    return verifyNodeSignature(node, buffer.array(), buffer.arrayOffset(), buffer.remaining());
  }
}
