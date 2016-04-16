package query;

import parser.AST_CreateIndex;
import relop.Schema;
import global.RID;
import heap.HeapFile;
import heap.HeapScan;
import index.HashIndex;
import global.Minibase;
import relop.Tuple;
import global.SearchKey;

/**
 * Execution plan for creating indexes.
 */
class CreateIndex implements Plan {

    /** Name of the table to create. */
    protected String fileName;

    /** Name of the table to index. */
    protected String ixTable;

    /** Name of the column to index. */
    protected String ixColumn;

    /** Schema of the table to create. */
    protected Schema schema;

    /** Column */
    protected int ixColumnNum;

  /**
   * Optimizes the plan, given the parsed query.
   *
   * @throws QueryException if index already exists or table/column invalid
   */
  public CreateIndex(AST_CreateIndex tree) throws QueryException {
    ixTable = tree.getIxTable();
    ixColumn = tree.getIxColumn();
    fileName = tree.getFileName();
    schema = QueryCheck.tableExists(ixTable);
    ixColumnNum = QueryCheck.columnExists(schema, ixColumn);
    QueryCheck.fileNotExists(fileName);
  } // public CreateIndex(AST_CreateIndex tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    // create the index
    HashIndex hashIndex = new HashIndex(fileName);

    // open existing file
    HeapFile heapFile = new HeapFile(ixTable);
    HeapScan heapScan = heapFile.openScan();
    RID rid = new RID();


    while(heapScan.hasNext()){
      byte[] recordData = heapScan.getNext(rid);
      Tuple tuple = new Tuple(schema, recordData);
      SearchKey searchKey = new SearchKey(tuple.getField(ixColumn));
      hashIndex.insertEntry(searchKey, rid);
    }

    // add the schema to the catalog
    Minibase.SystemCatalog.createIndex(fileName, ixTable, ixColumn);

  } // public void execute()

} // class CreateIndex implements Plan
