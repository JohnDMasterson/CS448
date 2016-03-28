package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.HashScan;

/**
 * Wrapper for hash scan, an index access method.
 */
public class KeyScan extends Iterator {

  private HashIndex index;
  private SearchKey key;
  private HeapFile file;
  private HashScan scan;
  private RID next;

  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public KeyScan(Schema schema, HashIndex index, SearchKey key, HeapFile file) {
    this.setSchema(schema);
    this.index = index;
    this.key = key;
    this.file = file;
    this.scan = index.openScan(key);
    next = null;
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    this.indent(depth);
    System.out.println("KeyScan");
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    scan.close();
    scan = index.openScan(key);
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return true;
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    scan.close();
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    if(next == null) {
      if(scan.hasNext()) {
        next = scan.getNext();
      }
    }
    return next != null;
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    if(hasNext()) {
      RID rid = next;
      next = null;
      byte[] recordData = file.selectRecord(rid);
      Schema schema = this.getSchema();
      return new Tuple(schema, recordData);
    }
    throw new IllegalStateException();
    //throw new UnsupportedOperationException("Not implemented");
  }

} // public class KeyScan extends Iterator
