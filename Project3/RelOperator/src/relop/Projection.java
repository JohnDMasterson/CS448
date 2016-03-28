package relop;

import java.util.LinkedList;

/**
 * The projection operator extracts columns from a relation; unlike in
 * relational algebra, this operator does NOT eliminate duplicate tuples.
 */
public class Projection extends Iterator {

  /**
   * Constructs a projection, given the underlying iterator and field numbers.
   */
  private Iterator iter;
  private LinkedList<Integer> fields;

  public Projection(Iterator iter, Integer... fields) {
    this.iter = iter;
    this.fields = new LinkedList();
    int length = fields.length;
    Schema postProj = new Schema(length);
    Schema preProj = iter.getSchema();
    for(int i=0; i<length; i++) {
      //add fields to my linked list
      int field = fields[i];
      this.fields.add(field);
      //setup schema
      postProj.initField(i, preProj, field);
    }
    this.setSchema(postProj);
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    this.indent(depth);
    System.out.println("Projection");
    iter.explain(depth +1);
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    iter.restart();
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if the iterator is open; false otherwise.
   */
  public boolean isOpen() {
    return iter.isOpen();
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Closes the iterator, releasing any resources (i.e. pinned pages).
   */
  public void close() {
    iter.close();
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Returns true if there are more tuples, false otherwise.
   */
  public boolean hasNext() {
    return iter.hasNext();
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the next tuple in the iteration.
   *
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    if(hasNext()) {
      Tuple ret = new Tuple(this.getSchema());
      Tuple preProj = iter.getNext();
      int i=0;
      for(i=0; i<fields.size(); i++){
        int field = fields.get(i);
        ret.setField(i, preProj.getField(field));
      }
      return ret;
    }
    throw new IllegalStateException();
    //throw new UnsupportedOperationException("Not implemented");
  }
} // public class Projection extends Iterator
