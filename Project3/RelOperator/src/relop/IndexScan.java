package relop;

import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import index.BucketScan;

/**
 * Wrapper for bucket scan, an index access method.
 */
public class IndexScan extends Iterator {

  private HeapFile file;
  private HashIndex index;
  private BucketScan bs;

  /**
   * Constructs an index scan, given the hash index and schema.
   */
  public IndexScan(Schema schema, HashIndex index, HeapFile file) {
    init(schema, index, file);
  }

  /**
   * Constructs an index scan, given an iterator and an index
   */
  public IndexScan(Iterator iter, int indexOn) {
    if(iter instanceof IndexScan) {
      // just cast it
      IndexScan scan = (IndexScan)iter;
      init(scan.getSchema(), scan.getHashIndex(), scan.getHeapFile());
    }else if(iter instanceof FileScan) {
      // cast to FileScan
      FileScan fs = (FileScan)iter;
      // get heapfile
      HeapFile file = fs.getHeapFile();
      // create temp index
      HashIndex index = new HashIndex(null);
      // adds all tuples to hashIndex
      while(fs.hasNext()) {
        Tuple tuple = fs.getNext();
        RID rid = fs.getLastRID();
        SearchKey key = new SearchKey(tuple.getField(indexOn));
        index.insertEntry(key, rid);
      }
      init(iter.getSchema(), index, file);
    }else {
      // create temp heapfile
      HeapFile file = new HeapFile(null);
      // create temp hasindex
      HashIndex index = new HashIndex(null);
      // adds all tuples to heap and index
      while(iter.hasNext()) {
        Tuple tuple = iter.getNext();
        RID rid = tuple.insertIntoFile(file);
        SearchKey key = new SearchKey(tuple.getField(indexOn));
        index.insertEntry(key, rid);
      }
      // create new indexscan
      init(iter.getSchema(), index, file);
    }
  }

  private void init(Schema schema, HashIndex index, HeapFile file) {
    this.setSchema(schema);
    this.index = index;
    this.file = file;
    this.bs = index.openScan();
    //throw new UnsupportedOperationException("Not implemented");
  }

  public HashIndex getHashIndex() {
    return this.index;
    //throw new UnsupportedOperationException("Not implemented");
  }

  public HeapFile getHeapFile() {
    return this.file;
    //throw new UnsupportedOperationException("Not implemented");
  }

  public Boolean findBucket(int hashIndex)  {
    this.restart();
    while(hasNext()) {
      if(getNextHash() == hashIndex) return true;
      getNext();
    }
    return false;
    //throw new UnsupportedOperationException("Not implemented");
  }



  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    this.indent(depth);
    System.out.println("IndexScan");
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    bs.close();
    bs = index.openScan();
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
    bs.close();
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    return bs.hasNext();
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the next tuple in the iteration.
   *
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    if(hasNext()) {
      RID rid = bs.getNext();
      byte[] recordData = file.selectRecord(rid);
      Tuple ret = new Tuple(this.getSchema(), recordData);
      return ret;
    }
    throw new IllegalStateException();
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the key of the last tuple returned.
   */
  public SearchKey getLastKey() {
    return bs.getLastKey();
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns the hash value for the bucket containing the next tuple, or maximum
   * number of buckets if none.
   */
  public int getNextHash() {
    return bs.getNextHash();
    //throw new UnsupportedOperationException("Not implemented");
  }

} // public class IndexScan extends Iterator
