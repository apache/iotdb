/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.tsfile.encoding.huffmanForByte;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.List;

/** Huffman compression encode bytes data
 * It first calculates the frequency of every byte in a byte array,
 * it then creates a binary tree using the frequency with shorter bit length
 * finally return the encoded message with sufficient information.
 * */
public class huffmanCompression {

    /*a dictionary to map each byte*/
    public static List<String> dictionary = new ArrayList<>();
    private huffmanNode root;

    public huffmanCompression(){}

    public huffmanNode getRoot(){return root;}

    /** build huffmanTree from bytesBuffer*/
    public void buildHuffmanTree(List<byte[]> bytesBuffer){
        List<Byte> uniqueByte = getUniqueBytes(bytesBuffer);
        List<Integer> frequency = getFrequency(bytesBuffer,uniqueByte);
        huffmanNode tempRoot = null;

        /*a list to store all huffman nodes*/
        List<huffmanNode> nodeList = new ArrayList<>();

        /*create huffman node for each character*/
        for (int i = 0; i< uniqueByte.size(); i++){
            huffmanNode node = new huffmanNode(frequency.get(i), uniqueByte.get(i),null,null);
            nodeList.add(node);
        }

        /*sort node list based on frequency*/
        nodeList = sortNode(nodeList);

        /*loop to merge two smallest frequency nodes together*/
        while(nodeList.size() >1){
            /*join the two least frequent nodes*/
            huffmanNode minNode = nodeList.get(0);
            nodeList.remove(0);
            huffmanNode secondMinNode = nodeList.get(0);
            nodeList.remove(0);
            /*the value is set to 11111111*/
            huffmanNode huffmanNodeJoint = new huffmanNode(minNode.frequency+secondMinNode.frequency,
                    (byte)0xFF,minNode,secondMinNode);
            tempRoot = huffmanNodeJoint;
            nodeList.add(tempRoot);
            nodeList = sortNode(nodeList);
        }

        for (huffmanNode node:nodeList){
            System.out.println(node.character +" has frequency of: " + node.frequency);
        }
        /*create a dictionary of codes*/
        createDictionary(tempRoot,"");
        root = tempRoot;
    }



    /** compress byte array.
     * Using a huffman tree to match a code for each byte,
     * output byte array*/
    public byte[] huffmanCompress(List<byte[]> bytesBuffer){

        String code = "";

        /*translate each byte in bytes buffer*/
        for (byte[] byteList: bytesBuffer){
            for (byte b: byteList){
                for (String word: dictionary){
                    if (word.getBytes()[0] == b){
                        code = code + word.substring(1);
                    }
                }
            }
        }
        /*since the code is string, we convert it back to binary*/
        return getBinary(code);
    }


    /** decompress byte array.
     * Using a huffman tree to output the translated code,
     * encodedBytes is a Byte Array we just encode*/
    public byte[] huffmanDecompress(huffmanNode root,byte[] encodedBytes){
        String decompress = "";
        huffmanNode rootCopy = root;
        String bitString = getString(encodedBytes);
        for(Character character : bitString.toCharArray()){
            if (character == '0') {
                root = root.left;
            }
            else if(character == '1') {
                root = root.right;
            }
            if(root.right == null && root.left == null){
                if(root.character == (byte)0xFF){
                    decompress = decompress + "\n";
                }
                else {
                    decompress = decompress + root.character;
                }
                root = rootCopy;
            }
        }

        return getBinary(decompress);
    }



    /**get the number of unique bytes in a buffer
     * bytesBuffer: a list of byte array
     * */
    public List<Byte> getUniqueBytes(List<byte[]> bytesBuffer){
        List<Byte> uniqueBytes = new ArrayList<>();
        for (byte[] byteList: bytesBuffer){
            for (byte b: byteList){
                if (!uniqueBytes.contains(b)){
                    uniqueBytes.add(b);
                    System.out.println("one unique byte is: " + b);
                }
            }
        }
        System.out.println("the number of unique byte is: "+uniqueBytes.size());
        return uniqueBytes;
    }


    /**get the frequency of each Byte
     *allBytes is a list of all unique characters
     */
    public List<Integer> getFrequency(List<byte[]> bytesBuffer, List<Byte> allBytes){
        List<Integer> frequency = new ArrayList<>();
        for (Byte B: allBytes){
            int counter = 0;
            for (byte[] byteList: bytesBuffer){
                for (byte b: byteList){
                    if (b == B){
                        counter += 1;
                    }
                }
            }
            frequency.add(counter);
        }
        return frequency;
    }


    /**sort huffman node so that the lowest frequency node comes first*/
    public List<huffmanNode> sortNode(List<huffmanNode> nodeList){
        for (int i =0;i<nodeList.size();i++){
            int pos = i;
            for (int j = i; j < nodeList.size(); j++){
                if (nodeList.get(j).frequency < nodeList.get(pos).frequency){
                    pos = j;
                }
            }
            huffmanNode minNode = nodeList.get(pos);
            nodeList.set(pos,nodeList.get(i));
            nodeList.set(i, minNode);
        }
        return nodeList;
    }


    /**assigning code for each byte*/
    public void createDictionary(huffmanNode root, String code){
        /*a byte is found*/
        if (root.left == null && root.right == null){
            dictionary.add(root.character+code);
            return;
        }
        /*recursively find code for elements in each subtree*/
        createDictionary(root.left,code+"0");
        createDictionary(root.right,code+"1");
    }


    /**produce a array of byte from encoding string*/
    public byte[] getBinary(String encodedString){
        StringBuilder sBuilder = new StringBuilder(encodedString);
        while (sBuilder.length() % 8 != 0) {
            sBuilder.append('0');
        }
        encodedString = sBuilder.toString();

        // a byte array to store the bit version of encodedString
        byte[] ByteCode = new byte[encodedString.length() / 8];

        // split string into 8 bits per byte and store in array
        for (int i = 0; i < encodedString.length(); i++) {
            char c = encodedString.charAt(i);
            if (c == '1') {
                ByteCode[i >> 3] |= 0x80 >> (i & 0x7);
            }
        }
        return ByteCode;
    }


    /**produce a string from encoded byte array*/
    static @NotNull String getString(byte[] bytes) {
        StringBuilder builder = new StringBuilder(bytes.length * Byte.SIZE);

        for (int i = 0; i < Byte.SIZE * bytes.length; i++) {
            // build string back of bits
            builder.append((bytes[i / Byte.SIZE] << i % Byte.SIZE & 0x80) == 0 ? '0' : '1');
        }
        return builder.toString();
    }


}
