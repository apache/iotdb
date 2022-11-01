/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package org.apache.iotdb.tsfile.encoding.encoderbuff;

/**
 *
 * @author Wang
 */
public interface Encoder {

    public byte[] encode(double[] origin);

    public double[] decode(byte[] bytes);
}
