/**
 * JWave is distributed under the MIT License (MIT); this file is part of.
 *
 * <p>Copyright (c) 2008-2024 Christian (graetz23@gmail.com)
 *
 * <p>Permission is hereby granted, free of charge, to any person obtaining a copy of this software
 * and associated documentation files (the "Software"), to deal in the Software without restriction,
 * including without limitation the rights to use, copy, modify, merge, publish, distribute,
 * sublicense, and/or sell copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * <p>The above copyright notice and this permission notice shall be included in all copies or
 * substantial portions of the Software.
 *
 * <p>THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING
 * BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM,
 * DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.
 */
package org.apache.iotdb.db.query.simpiece.jwave.datatypes.blocks;

import org.apache.iotdb.db.query.simpiece.jwave.datatypes.lines.Line;
import org.apache.iotdb.db.query.simpiece.jwave.datatypes.lines.LineHash;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveException;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveFailure;

import java.util.HashMap;

/**
 * Uses HashMap generic for sparse data representations.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 16.05.2015 16:41:53
 */
public class BlockHash extends Block {

  /**
   * Storing LineHash objects in a HashMap for sparse representation.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 16:43:03
   */
  HashMap<Integer, Line> _hashMapLines = null;

  /**
   * Create an object of a sub type; e.g. as pattern.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 13:58:16
   */
  public BlockHash() {
    super();
  } // BlockHash

  /**
   * Copy constructor that takes over - if available - the values of another type of block.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 13:59:49
   * @param block object of type block; e.g. BlockFull
   */
  public BlockHash(Block block) {

    super(block); // takes the off sets and the dimension

    try {
      alloc();
      for (int i = 0; i < _noOfRows; i++)
        for (int j = 0; j < _noOfCols; j++) set(i, j, block.get(i, j));
    } catch (JWaveException e) {
      e.printStackTrace();
    } // try

    // TODO optimize this constructor by instanceof

  } // BlockHash

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 16:41:53
   * @param noOfRows
   * @param noOfCols
   */
  public BlockHash(int noOfRows, int noOfCols) {
    super(noOfRows, noOfCols);
  } // BlockHash

  /**
   * Passing information that takes the block as a part of a global structure; e.g. a SuperBlock.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 14:03:36
   * @param offSetRow the global off set of rows of the block
   * @param offSetCol the global off set of columns of the block
   * @param noOfRows the number of rows of the block
   * @param noOfCols the number of columns of the block
   */
  public BlockHash(int offSetRow, int offSetCol, int noOfRows, int noOfCols) {
    super(offSetRow, offSetCol, noOfRows, noOfCols);
  } // BlockHash

  /*
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 15:13:30 (non-Javadoc)
   * @see jwave.datatypes.Super#copy()
   */
  @Override
  public Block copy() {
    return new BlockHash(this);
  } // copy

  /*
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 15:02:40 (non-Javadoc)
   * @see jwave.datatypes.Super#isAllocated()
   */
  @Override
  public boolean isAllocated() {
    boolean isAllocated = true;
    if (_hashMapLines == null) isAllocated = false;
    return isAllocated;
  } // isAllocated

  /*
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 15:02:40 (non-Javadoc)
   * @see jwave.datatypes.Super#alloc()
   */
  @Override
  public void alloc() throws JWaveException {
    if (!isAllocated()) _hashMapLines = new HashMap<Integer, Line>();
  } // alloc

  /*
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 15:02:40 (non-Javadoc)
   * @see jwave.datatypes.Super#erase()
   */
  @Override
  public void erase() throws JWaveException {
    if (_hashMapLines != null) _hashMapLines.clear();
    _hashMapLines = null;
  } // erase

  /*
   * Getter!
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 16:41:53 (non-Javadoc)
   * @see jwave.datatypes.blocks.Block#get(int, int)
   */
  @Override
  public double get(int i, int j) throws JWaveException {

    checkMemory();

    check(i, j);

    Line line = null;
    double value = 0.;

    if (_hashMapLines.containsKey(j)) {

      line = _hashMapLines.get(j);

      value = line.get(i);

    } else throw new JWaveFailure("Line - no value stored for requested i: " + i);

    return value;
  } // get

  /*
   * Setter!
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 16:41:53 (non-Javadoc)
   * @see jwave.datatypes.blocks.Block#set(int, int, double)
   */
  @Override
  public void set(int i, int j, double value) throws JWaveException {

    checkMemory();

    check(i, j);

    Line line = null;

    if (_hashMapLines.containsKey(j)) {

      line = _hashMapLines.get(j);
      line.set(i, value);

    } else {

      line = new LineHash(_offSetRow, _noOfRows);
      line.alloc();
      line.set(i, value);
      _hashMapLines.put(j, line);
    } // if
  } // set
} // class
