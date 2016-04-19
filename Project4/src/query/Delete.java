package query;

import parser.AST_Delete;
import relop.Predicate;
import heap.HeapFile;
import index.HashIndex;
import global.Minibase;

/**
 * Execution plan for deleting tuples.
 */
class Delete implements Plan {

  protected String fileName;
  protected Predicate[][] predicates;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if table doesn't exist or predicates are invalid
   */
  public Delete(AST_Delete tree) throws QueryException {
    fileName = tree.getFileName();
    predicates = tree.getPredicates();
  } // public Delete(AST_Delete tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    HashIndex hashIndex = new HashIndex(fileName);
    hashIndex.deleteFile();

    HeapFile heapFile = new HeapFile(fileName);
    heapFile.deleteFile();

    // print the output message
    System.out.println("1 rows affected.");

  } // public void execute()

} // class Delete implements Plan
