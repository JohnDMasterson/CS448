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
import java.io.File;

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

		// initialize schema for the "Employee" table
		employee = new Schema(5);
		employee.initField(0, AttrType.INTEGER, 4, "EmpId");
		employee.initField(1, AttrType.STRING, 20, "Name");
		employee.initField(2, AttrType.INTEGER, 4, "Age");
		employee.initField(3, AttrType.INTEGER, 4, "Salary");
		employee.initField(4, AttrType.INTEGER, 4, "DeptID");

		// initialize schema for the "Rides" table
		dept = new Schema(4);
		dept.initField(0, AttrType.INTEGER, 4, "DeptId");
		dept.initField(1, AttrType.STRING, 10, "Name");
		dept.initField(2, AttrType.INTEGER, 4, "MinSalary");
		dept.initField(3, AttrType.INTEGER, 4, "MaxSalary");

		// run all the test cases
		System.out.println("\n" + "Running " + TEST_NAME + "...");
		boolean status = PASS;
		status &= qep.test1();
		status &= qep.test2();
		status &= qep.test3();
		status &= qep.test4();

		// display the final results
		System.out.println();
		if (status != PASS) {
			System.out.println("Error(s) encountered during " + TEST_NAME + ".");
		} else {
			System.out.println("All " + TEST_NAME
					+ " completed; verify output for correctness.");
		}

	} // public static void main (String argv[])

	protected boolean test1() {
		try {
			System.out.println("\nTest 1: Display for each employee his ID, Name and Age");
			
			String name = "./src/tests/SampleData/Employee.txt";
			HeapFile hpfile = new HeapFile(name);
			hpfile.openScan();
			System.out.println(hpfile.toString() + " getRecCnt=" + hpfile.getRecCnt());
			FileScan scan = new FileScan(employee, hpfile);
			Projection pro = new Projection(scan, 0,1,2);
			pro.execute();
			System.out.print("\n\nTest 1 completed without exception.");
			return PASS;
		} catch (Exception exc) {
			exc.printStackTrace(System.out);
			System.out.print("\n\nTest 1 terminated because of exception.");
			return FAIL;
		} finally {
			printSummary(6);
			System.out.println();
		}
	}	// protected boolean test1()

	protected boolean test2() {
		try {
			System.out.println("\nTest 2: Display the Name for the departments with MinSalary = MaxSalary");


			System.out.print("\n\nTest 2 completed without exception.");
			return PASS;

		} catch (Exception exc) {
			exc.printStackTrace(System.out);
			System.out.print("\n\nTest 2 terminated because of exception.");
			return FAIL;
		} finally {
			printSummary(6);
			System.out.println();
		}
	}	// protected boolean test2()

	protected boolean test3() {
		try {
			System.out.println("\nTest 3: For each employee, display his Name and the Name of his department as well as the maximum salary of his department");
			initCounts();
			saveCounts(null);


			System.out.print("\n\nTest 3 completed without exception.");
			return PASS;

		} catch (Exception exc) {
			exc.printStackTrace(System.out);
			System.out.print("\n\nTest 3 terminated because of exception.");
			return FAIL;
		} finally {
			printSummary(6);
			System.out.println();
		}
	}	// protected boolean test3()

	protected boolean test4() {
		try {
			System.out.println("\nTest 4: Display the Name for each employee whose Salary is greater than the maximum salary of his department");
			initCounts();
			saveCounts(null);


			System.out.print("\n\nTest 4 completed without exception.");
			return PASS;
		} catch (Exception exc) {
			exc.printStackTrace(System.out);
			System.out.print("\n\nTest 4 terminated because of exception.");
			return FAIL;
		} finally {
			printSummary(6);
			System.out.println();
		}
	}	// protected boolean test4()

}
