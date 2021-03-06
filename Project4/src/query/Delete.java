package query;

import parser.AST_Delete;
import relop.Predicate;
import global.RID;
import heap.HeapFile;
import heap.HeapScan;
import index.HashIndex;
import global.Minibase;
import relop.Tuple;
import global.SearchKey;
import relop.Schema;

/**
 * Execution plan for deleting tuples.
 */
class Delete implements Plan {

  protected String fileName;
  protected Predicate[][] predicates;
  protected Schema schema;

  /**
   * Optimizes the plan, given the parsed query.
   *
   * @throws QueryException if table doesn't exist or predicates are invalid
   */
  public Delete(AST_Delete tree) throws QueryException {
    fileName = tree.getFileName();
    predicates = tree.getPredicates();
    schema = QueryCheck.tableExists(fileName);
    QueryCheck.predicates(schema, predicates);
  } // public Delete(AST_Delete tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    HeapFile heapfile = new HeapFile(fileName);
    HeapScan heapscan = heapfile.openScan();
    RID rid = new RID();
    int count = 0;
    boolean passOR = false;
    boolean passAND = true;

    while (heapscan.hasNext())
    {
      passAND = true;
      Tuple tuple = new Tuple(schema, heapscan.getNext(rid));
      for (int i=0; i<predicates.length && passAND; i++)
      {
        passOR = false;
        for (int j=0; j<predicates[i].length && !passOR; j++)
        {
            if (predicates[i][j].evaluate(tuple)){
              passOR = true;
            }
        }
        if (!passOR)
          passAND = false;
      }

      if (passAND)
      {
        heapfile.deleteRecord(rid);
        IndexDesc[] inds = Minibase.SystemCatalog.getIndexes(fileName);
        for(int i=0; i<inds.length; i++)
        {
          IndexDesc index = inds[i];
          HashIndex hashIndex = new HashIndex(index.indexName);
          SearchKey key = new SearchKey(tuple.getField(index.columnName));
          hashIndex.deleteEntry(key, rid);
        } // end for
        count++;
      } // end if
    } // end while
    heapscan.close();

    // print the output message
    System.out.println(count + " rows affected.");

  } // public void execute()

} // class Delete implements Plan
