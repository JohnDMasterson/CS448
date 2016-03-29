package tests;

// YOUR CODE FOR PART3 SHOULD GO HERE.
import global.AttrOperator;
import global.AttrType;
import global.RID;
import global.SearchKey;
import heap.HeapFile;
import index.HashIndex;
import relop.FileScan;
import relop.HashJoin;
import relop.IndexScan;
import relop.KeyScan;
import relop.Predicate;
import relop.Projection;
import relop.Schema;
import relop.Selection;
import relop.SimpleJoin;
import relop.Tuple;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.io.BufferedReader;
import java.lang.Integer;

public class QEPTest extends TestDriver {

	/** The display name of the test suite. */
	private static final String TEST_NAME = "query evaluation pipeline tests";

	/** Drivers table schema. */
	private static Schema employee;

	/** Rides table schema. */
	private static Schema dept;

	public static void main(String argv[]) {

		QEPTest qep = new QEPTest();
		qep.create_minibase();

		// --------- Employee ------------------------------------------
		// init schema
		employee = new Schema(5);
		employee.initField(0, AttrType.INTEGER, 4, "EmpId");
		employee.initField(1, AttrType.STRING, 20, "Name");
		employee.initField(2, AttrType.INTEGER, 4, "Age");
		employee.initField(3, AttrType.INTEGER, 4, "Salary");
		employee.initField(4, AttrType.INTEGER, 4, "DeptID");

		// parse input file
		String empName = argv[0] + "/Employee.txt";
		HeapFile empHeap = new HeapFile("emp");
		Tuple tuple1 = new Tuple(employee);
		try {
			BufferedReader read = Files.newBufferedReader(Paths.get(empName));
			read.readLine();	// omit first line
			String l;
			while((l = read.readLine()) != null){
				// create the tuple
				tuple1.setIntFld(0, Integer.parseInt(l.split(",")[0].replaceAll("\\D+","")));
				tuple1.setStringFld(1, l.split(",")[1]);
				tuple1.setIntFld(2, Integer.parseInt(l.split(",")[2].replaceAll("\\D+","")));
				tuple1.setIntFld(3, Integer.parseInt(l.split(",")[3].replaceAll("\\D+","")));
				tuple1.setIntFld(4, Integer.parseInt(l.split(",")[4].replaceAll("\\D+","")));

				// insert the tuple in the file
				RID rid = empHeap.insertRecord(tuple1.getData());
			} // for
		} catch (Exception e)
		{
			e.printStackTrace(System.out);
			System.out.print("\n\nCouldn't read input file - IOException.");
		}

		// ------------- Department --------------------------------------
		// init schema
		dept = new Schema(4);
		dept.initField(0, AttrType.INTEGER, 4, "DeptId");
		dept.initField(1, AttrType.STRING, 10, "Name");
		dept.initField(2, AttrType.INTEGER, 4, "MinSalary");
		dept.initField(3, AttrType.INTEGER, 4, "MaxSalary");

		// parse input file
		String deptName = argv[0] + "/Department.txt";
		HeapFile deptHeap = new HeapFile("dept");
		Tuple tuple2 = new Tuple(dept);
		try {
			BufferedReader read = Files.newBufferedReader(Paths.get(deptName));
			read.readLine();	// omit first line
			String l;
			while((l = read.readLine()) != null){
				// create the tuple
				tuple2.setIntFld(0, Integer.parseInt(l.split(",")[0].replaceAll("\\D+","")));
				tuple2.setStringFld(1, l.split(",")[1]);
				tuple2.setIntFld(2, Integer.parseInt(l.split(",")[2].replaceAll("\\D+","")));
				tuple2.setIntFld(3, Integer.parseInt(l.split(",")[3].replaceAll("\\D+","")));

				// insert the tuple in the file and index
				RID rid = deptHeap.insertRecord(tuple2.getData());
			} // for
		} catch (Exception e)
		{
			e.printStackTrace(System.out);
			System.out.print("\n\nCouldn't read input file - IOException.");

		}

		// run all the test cases
		System.out.println("\n" + "Running " + TEST_NAME + "...");
		boolean status = PASS;
		status &= qep.test1(empHeap);
		status &= qep.test2(deptHeap);
		status &= qep.test3(empHeap, deptHeap);
		status &= qep.test4(empHeap, deptHeap);

		// display the final results
		System.out.println();
		if (status != PASS) {
			System.out.println("Error(s) encountered during " + TEST_NAME + ".");
		} else {
			System.out.println("All " + TEST_NAME
					+ " completed; verify output for correctness.");
		}

	} // public static void main (String argv[])

	protected boolean test1(HeapFile empFile) {
		try {
			System.out.println("\nTest 1: Display for each employee his ID, Name and Age");
			
			FileScan scan = new FileScan(employee, empFile);
			Projection pro = new Projection(scan, 0,1,2);
			pro.execute();
			System.out.print("\n\nTest 1 completed without exception.");
			return PASS;
		} catch (Exception exc) {
			exc.printStackTrace(System.out);
			System.out.print("\n\nTest 1 terminated because of exception.");
			return FAIL;
		} finally {
			System.out.println();
		}
	}	// protected boolean test1()

	protected boolean test2(HeapFile deptFile) {
		try {
			System.out.println("\nTest 2: Display the Name for the departments with MinSalary = MaxSalary");

			FileScan scan = new FileScan(dept, deptFile);
			Predicate sPred = new Predicate(AttrOperator.EQ,
					AttrType.COLNAME, "MinSalary", AttrType.COLNAME, "MaxSalary");
			Selection sel = new Selection(scan, sPred);
			Projection pro = new Projection(sel, 1,2,3);
			pro.execute();

			System.out.print("\n\nTest 2 completed without exception.");
			return PASS;

		} catch (Exception exc) {
			exc.printStackTrace(System.out);
			System.out.print("\n\nTest 2 terminated because of exception.");
			return FAIL;
		} finally {
			System.out.println();
		}
	}	// protected boolean test2()

	protected boolean test3(HeapFile empFile, HeapFile deptFile) {
		try {
			System.out.println("\nTest 3: For each employee, display his Name and the Name of his department as well as the maximum salary of his department");

			HashJoin join = new HashJoin(new FileScan(employee, empFile),new FileScan(dept, deptFile), 4,0);
			Projection pro = new Projection(join, 1, 6, 8);
			pro.execute();

			System.out.print("\n\nTest 3 completed without exception.");
			return PASS;

		} catch (Exception exc) {
			exc.printStackTrace(System.out);
			System.out.print("\n\nTest 3 terminated because of exception.");
			return FAIL;
		} finally {
			System.out.println();
		}
	}	// protected boolean test3()

	protected boolean test4(HeapFile empFile, HeapFile deptFile) {
		try {
			System.out.println("\nTest 4: Display the Name for each employee whose Salary is greater than the maximum salary of his department");

			HashJoin join = new HashJoin(new FileScan(employee, empFile),new FileScan(dept, deptFile), 4,0);
			Predicate sPred = new Predicate(AttrOperator.GT,AttrType.COLNAME, "Salary", AttrType.COLNAME, "MaxSalary");
			Selection sel = new Selection(join, sPred);
			Projection pro = new Projection(sel, 1);
			pro.execute();
			System.out.print("\n\nTest 4 completed without exception.");
			return PASS;
		} catch (Exception exc) {
			exc.printStackTrace(System.out);
			System.out.print("\n\nTest 4 terminated because of exception.");
			return FAIL;
		} finally {
			System.out.println();
		}
	}	// protected boolean test4()

}
