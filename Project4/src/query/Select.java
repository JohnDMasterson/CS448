package query;

import parser.AST_Select;
import heap.HeapFile;
import global.Minibase;
import relop.Projection;
import relop.Predicate;
import relop.Iterator;
import relop.Schema;
import relop.SimpleJoin;
import relop.Selection;
import relop.FileScan;

/**
 * Execution plan for selecting tuples.
 */
class Select implements Plan {

  protected Iterator selectIterator = null;
  protected boolean explain = false;

  /**
   * Optimizes the plan, given the parsed query.
   *
   * @throws QueryException if validation fails
   */
  public Select(AST_Select tree) throws QueryException {
    String[] tables = tree.getTables();
    String[] columns = tree.getColumns();
    Predicate[][] predicates = tree.getPredicates();
    explain = tree.isExplain;
    if (tables == null)
      throw new QueryException("SELECT: Tables invalid");
    if (columns == null)
      throw new QueryException("SELECT: Columns invalid");
    if (tables.length == 0)
      throw new QueryException("SELECT: Minimum 1 table required");

    // Build schema
    Schema[] tableSchemas = new Schema[tables.length];
    Schema schema = QueryCheck.tableExists(tables[0]);
    tableSchemas[0] = schema;
    for (int i=1; i<tables.length; i++)
    {
      tableSchemas[i] = QueryCheck.tableExists(tables[0]);
      schema = Schema.join(schema, tableSchemas[i]);
    }
    QueryCheck.predicates(schema, predicates);

    // Check columns
    Integer[] col = null;
    if (columns.length > 0)
    {
      col = new Integer[columns.length];
      for (int c=0; c<columns.length; c++)
        col[c] = QueryCheck.columnExists(schema, columns[c]);
    }

    // Filescan and Simplejoin
    Iterator iter = new FileScan(tableSchemas[0],new HeapFile(tables[0]));
    for (int ind = 1; ind < tables.length; ind++)
    {
      Iterator fs = new FileScan(tableSchemas[ind], new HeapFile(tables[ind]));
      iter = new SimpleJoin(iter, fs);
    }

    // Selection and Projection
    for (int i=0; i < predicates.length; i++){
      // We can split this in a for loop because every new selection is AND'd on top of the previous one
      iter = new Selection(iter, predicates[i]);
    }
    if (columns.length > 0){
      iter = new Projection(iter, col);
    }

    selectIterator = iter;
  } // public Select(AST_Select tree) throws QueryException

  /**
   * Executes the plan and prints applicable output.
   */
  public void execute() {
    int ret = 0;
    if (selectIterator != null)
    {
      if (explain)
        selectIterator.explain(0);
      else
      {
        ret = selectIterator.execute();
        // print the output message
        System.out.println(ret + " rows selected.");
      }
    }

    selectIterator.close();

  } // public void execute()

} // class Select implements Plan
