package relop;

import global.RID;
import heap.HeapFile;
import heap.HeapScan;

/**
 * Wrapper for heap file scan, the most basic access method. This "iterator"
 * version takes schema into consideration and generates real tuples.
 */
public class FileScan extends Iterator {

  private HeapFile file;
  private HeapScan heapScan;
  private RID lastRID;

  /**
   * Constructs a file scan, given the schema and heap file.
   */
  public FileScan(Schema schema, HeapFile file) {
    this.setSchema(schema);
    this.file = file;
    this.heapScan = file.openScan();
    this.lastRID = new RID();
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    this.indent(depth);
    System.out.println("FileScan");
    //throw new UnsupportedOperationException("Not implemented");
  }
  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    heapScan.close();
    heapScan = file.openScan();
    this.lastRID = new RID();
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
    heapScan.close();
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    return heapScan.hasNext();
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    if(hasNext()) {
      byte[] recordData = heapScan.getNext(lastRID);
      Schema schema = this.getSchema();
      return new Tuple(schema, recordData);
    }
    throw new IllegalStateException();
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the RID of the last tuple returned.
   */
  public RID getLastRID() {
    return lastRID;
    //throw new UnsupportedOperationException("Not implemented");
  }


  public HeapFile getHeapFile(){
    return this.file;
  }

} // public class FileScan extends Iterator
