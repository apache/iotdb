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
import org.apache.iotdb.db.query.simpiece.jwave.datatypes.lines.LineFull;
import org.apache.iotdb.db.query.simpiece.jwave.exceptions.JWaveException;

/**
 * A block that uses a full array for storage of Line objects.
 *
 * @author Christian (graetz23@gmail.com)
 * @date 16.05.2015 14:45:52
 */
public class BlockFull extends Block {

  /**
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:26:11
   */
  protected Line[] _arrLines = null;

  /**
   * Create an object of a sub type; e.g. as pattern.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 13:57:28
   */
  public BlockFull() {
    super();
  } // BlockFull

  /**
   * Copy constructor that takes over - if available - the values of another type of block.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 14:01:22
   * @param block object of type block; e.g. BlockHash
   */
  public BlockFull(Block block) {

    super(block); // takes the off sets and the dimension

    try {
      alloc();
      for (int i = 0; i < _noOfRows; i++)
        for (int j = 0; j < _noOfCols; j++) set(i, j, block.get(i, j));
    } catch (JWaveException e) {
      e.printStackTrace();
    } // try

    // TODO optimize this constructor by instanceof

  } // BlockFull

  /**
   * Constructor setting members for and allocating memory!
   *
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 14:45:52
   * @param noOfRows
   * @param noOfCols
   */
  public BlockFull(int noOfRows, int noOfCols) {
    super(noOfRows, noOfCols); // store does exclusively
  } // BlockFull

  /**
   * Passing information that takes the block as a part of a global structure; e.g. a SuperBlock.
   *
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 14:05:55
   * @param offSetRow the global off set of rows of the block
   * @param offSetCol the global off set of columns of the block
   * @param noOfRows the number of rows of the block
   * @param noOfCols the number of columns of the block
   */
  public BlockFull(int offSetRow, int offSetCol, int noOfRows, int noOfCols) {
    super(offSetRow, offSetCol, noOfRows, noOfCols);
  } // BlockFull

  /*
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 15:12:35 (non-Javadoc)
   * @see jwave.datatypes.Super#copy()
   */
  @Override
  public Block copy() {
    return new BlockFull(this);
  } // copy

  /*
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 15:01:42 (non-Javadoc)
   * @see jwave.datatypes.Super#isAllocated()
   */
  @Override
  public boolean isAllocated() {
    boolean isAllocated = true;
    if (_arrLines == null) isAllocated = false;
    return isAllocated;
  } // isAllocated

  /*
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 15:01:42 (non-Javadoc)
   * @see jwave.datatypes.Super#alloc()
   */
  @Override
  public void alloc() throws JWaveException {
    if (!isAllocated()) {
      _arrLines = new Line[_noOfCols];
      for (int j = 0; j < _noOfCols; j++) {
        Line line = new LineFull(_offSetRow, _noOfRows);
        line.alloc();
        _arrLines[j] = line;
      } // for
    } // if
  } // alloc

  /*
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 15:01:42 (non-Javadoc)
   * @see jwave.datatypes.Super#erase()
   */
  @Override
  public void erase() throws JWaveException {
    if (_arrLines != null) {
      for (int j = 0; j < _noOfCols; j++)
        if (_arrLines[j] != null) {
          Line line = _arrLines[j];
          line.erase();
          _arrLines[j] = null;
        } // for
      _arrLines = null;
    } // if
  } // erase

  /*
   * Getter!
   * @author Christian (graetz23@gmail.com)
   * @date 16.05.2015 15:34:59 (non-Javadoc)
   * @see jwave.datatypes.blocks.Block#get(int, int)
   */
  @Override
  public double get(int i, int j) throws JWaveException {
    checkMemory();
    // check( j );
    check(i, j);
    Line line = _arrLines[j];
    double value = line.get(i);
    return value; // checks i again
  } // get

  /*
   * Setter!
   * @author Christian (graetz23@gmail.com)
   * @date 24.05.2015 15:24:39 (non-Javadoc)
   * @see jwave.datatypes.blocks.Block#set(int, int, double)
   */
  @Override
  public void set(int i, int j, double value) throws JWaveException {
    checkMemory();
    // check( j );
    check(i, j);
    Line line = _arrLines[j];
    line.set(i, value); // checks i again
  } // set
} // class
