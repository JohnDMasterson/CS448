package query;

import parser.AST_Insert;
import relop.Schema;
import global.RID;
import heap.HeapFile;
import heap.HeapScan;
import index.HashIndex;
import global.Minibase;
import relop.Tuple;
import global.SearchKey;

/**
 * Execution plan for inserting tuples.
 */
class Insert implements Plan {

  protected String fileName;
  protected Object[] values;
  protected Schema schema;

  /**
   * Optimizes the plan, given the parsed query.
   *
   * @throws QueryException if table doesn't exists or values are invalid
   */
  public Insert(AST_Insert tree) throws QueryException {
    fileName = tree.getFileName();
    values = tree.getValues();
    schema = QueryCheck.tableExists(fileName);
    QueryCheck.insertValues(schema, values);
  } // public Insert(AST_Insert tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    HeapFile heapFile = new HeapFile(fileName);
    Tuple tuple = new Tuple(schema, values);
    RID rid = heapFile.insertRecord(tuple.getData());
    IndexDesc[] inds = Minibase.SystemCatalog.getIndexes(fileName);
    for(int i=0; i<inds.length; i++) {
      IndexDesc index = inds[i];
      HashIndex hashIndex = new HashIndex(index.indexName);
      SearchKey key = new SearchKey(tuple.getField(index.columnName));
      hashIndex.insertEntry(key, rid);
    }
    // print the output message
    System.out.println("1 row inserted.");
  } // public void execute()

} // class Insert implements Plan
