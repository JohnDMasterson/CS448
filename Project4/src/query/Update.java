package query;

import parser.AST_Update;
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
 * Execution plan for updating tuples.
 */
class Update implements Plan {

  protected String fileName;
  protected Predicate[][] predicates;
  protected Schema schema;
  protected Object[] values;
  protected String[] columns;

  /**
   * Optimizes the plan, given the parsed query.
   * 
   * @throws QueryException if invalid column names, values, or pedicates
   */
  public Update(AST_Update tree) throws QueryException {
    fileName = tree.getFileName();
    predicates = tree.getPredicates();
    values = tree.getValues();
    schema = QueryCheck.tableExists(fileName);
    columns = tree.getColumns();

    // QueryCheck.insertValues(schema, values);
  } // public Update(AST_Update tree) throws QueryException

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
      Tuple tuple = new Tuple(schema, heapscan.getNext(rid));

      // Check to see if predicates hold
      passAND = true;
      for (int i=0; i<predicates.length && passAND; i++)
      {
        passOR = false;
        for (int j=0; j<predicates[i].length && !passOR; j++)
        {
          if (predicates[i][j].validate(schema)){
            if (predicates[i][j].evaluate(tuple)){
              passOR = true;
            }
          }
        }
        if (!passOR)
          passAND = false;
      } // end predicate check

      if (passAND)
      {
        // First, delete all old records
        IndexDesc[] inds = Minibase.SystemCatalog.getIndexes(fileName);
        for(int i=0; i<inds.length; i++)
        {
          IndexDesc index = inds[i];
          HashIndex hashIndex = new HashIndex(index.indexName);
          SearchKey key = new SearchKey(index.columnName);
          hashIndex.deleteEntry(key, rid);
        } // end for

        // Next, insert updated ones
        for (int i=0; i<values.length; i++) {
          tuple.setField(columns[i], values[i]);
        }
        heapfile.updateRecord(rid, tuple.getData());

        for(int j=0; j<inds.length; j++)
        {
          IndexDesc index = inds[j];
          HashIndex hashIndex = new HashIndex(index.indexName);
          SearchKey key = new SearchKey(tuple.getField(index.columnName));
          hashIndex.insertEntry(key, rid);
        } // end for
        count++;
      } // end if
    } // end while

    // print the output message
    System.out.println(count + " rows affected.");

  } // public void execute()

} // class Update implements Plan
