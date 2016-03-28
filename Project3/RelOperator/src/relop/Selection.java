package relop;

import java.util.LinkedList;

/**
 * The selection operator specifies which tuples to retain under a condition; in
 * Minibase, this condition is simply a set of independent predicates logically
 * connected by OR operators.
 */
public class Selection extends Iterator {

  /**
   * Constructs a selection, given the underlying iterator and predicates.
   */
  private Tuple next;
  private Iterator iter;
  private LinkedList<Predicate> preds;

  public Selection(Iterator iter, Predicate... preds) {
    this.iter = iter;
    this.preds = new LinkedList<Predicate>();
    for(int i=0; i<preds.length; i++) {
      this.preds.add(preds[i]);
    }
    next = null;
    this.setSchema(iter.getSchema());
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gives a one-line explaination of the iterator, repeats the call on any
   * child iterators, and increases the indent depth along the way.
   */
  public void explain(int depth) {
    this.indent(depth);
    System.out.println("Selection");
    iter.explain(depth +1);
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Restarts the iterator, i.e. as if it were just constructed.
   */
  public void restart() {
    next = null;
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
    if(next != null) return true;
    while(iter.hasNext()) {
      next = iter.getNext();
      for(int i=0; i<preds.size(); i++) {
        if(preds.get(i).evaluate(next)) return true;
      }
    }
    next = null;
    return false;
    //throw new UnsupportedOperationException("Not implemented");
  }

  /**
   * Gets the next tuple in the iteration.
   * 
   * @throws IllegalStateException if no more tuples
   */
  public Tuple getNext() {
    if(hasNext()) {
      Tuple ret = next;
      next = null;
      return ret;
    }
    throw new IllegalStateException();
    //throw new UnsupportedOperationException("Not implemented");
  }

} // public class Selection extends Iterator
