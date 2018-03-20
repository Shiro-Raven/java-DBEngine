package team10;

import java.util.Hashtable;
import java.util.Iterator;

public class Tests {

	static String tblName = "mockTable0";

	static void testDenseIndex(String tblName, String colName) {

		int denseIndexPageNumber = 1;
		Object lastValue = '\n';

		while (true) {

			Page denseIndexPage = PageManager.loadPageIfExists(
					"data/" + tblName + "/" + colName + "/indices/Dense/page_" + denseIndexPageNumber++ + ".ser");

			if (denseIndexPage == null) {

				System.out.println("Dense Check Is Complete!");
				return;

			}

			// Looping Through Rows Of EACH Index File
			for (int i = 0; i < denseIndexPage.getMaxRows() && denseIndexPage.getRows()[i] != null; i++) {

				Hashtable<String, Object> denseIndexRow = denseIndexPage.getRows()[i];

				Page tablePage = PageManager.loadPageIfExists(
						"data/" + tblName + "/page_" + ((Integer) denseIndexRow.get("pageNumber")) + ".ser");

				if (tablePage == null) {

					System.out.println("NOT FOUND: " + denseIndexRow);
					continue;

				}

				// Checking Not Equality And Deletion
				Hashtable<String, Object> tableRow = tablePage.getRows()[(Integer) denseIndexRow.get("locInPage")];
				if (!denseIndexRow.get("value").equals(tableRow.get(colName)))
					System.out.println("NOT EQUAL: " + denseIndexRow.get("value") + " & " + tableRow.get(colName));
				if (!denseIndexRow.get("isDeleted").equals(tableRow.get("isDeleted")))
					System.out.println("DELETION ERROR: " + denseIndexRow + " & " + tableRow);

				// Checking Increasing Order Of Dense Indices
				if (lastValue.toString().compareTo(denseIndexRow.get("value").toString()) > 0)
					System.out.println("ERROR WITH ORDER: " + lastValue.toString() + " & "
							+ denseIndexRow.get("value").toString());
				lastValue = denseIndexRow.get("value");

			}

		}

	}

	static void testBRINIndex(String tblName, String colName) {

		int BRINIndexPageNumber = 1;
		Object lastValue = '\n';

		while (true) {

			Page BRINIndexPage = PageManager.loadPageIfExists(
					"data/" + tblName + "/" + colName + "/indices/BRIN/page_" + BRINIndexPageNumber++ + ".ser");

			if (BRINIndexPage == null) {

				System.out.println("BRIN Check Is Complete!");
				return;

			}

			// Looping Through Rows Of EACH Index File
			for (int i = 0; i < BRINIndexPage.getMaxRows() && BRINIndexPage.getRows()[i] != null; i++) {

				Hashtable<String, Object> BRINIndexRow = BRINIndexPage.getRows()[i];
				Page tempPage = PageManager.loadPageIfExists("data/" + tblName + "/" + colName + "/indices/Dense/page_"
						+ ((int) BRINIndexRow.get("pageNumber")) + ".ser");

				int lastIndex;
				for (lastIndex = -1; lastIndex < (tempPage.getMaxRows() - 1)
						&& tempPage.getRows()[lastIndex + 1] != null; lastIndex++)
					;

				if (!tempPage.getRows()[0].get("value").equals(BRINIndexRow.get(colName + "Min")))
					System.out.println("Error With Initial Values: " + BRINIndexRow + " & " + tempPage.getRows()[0]);
				if (!tempPage.getRows()[lastIndex].get("value").equals(BRINIndexRow.get(colName + "Max")))
					System.out.println(
							"Error With Final Values: " + BRINIndexRow + " & " + tempPage.getRows()[lastIndex]);

				if (BRINIndexRow.get(colName + "Min").toString().toString()
						.compareTo(BRINIndexRow.get(colName + "Max").toString()) > 0)
					System.out.println("ERROR WITH ORDER of Tuple: " + lastValue.toString() + " & "
							+ BRINIndexRow.get(colName + "Max").toString());

				// Checking Increasing Order Of BRIN Indices
				if (lastValue.toString().compareTo(BRINIndexRow.get(colName + "Max").toString()) > 0)
					System.out.println("ERROR WITH ORDER: " + lastValue.toString() + " & "
							+ BRINIndexRow.get(colName + "Max").toString());
				lastValue = BRINIndexRow.get(colName + "Max");

			}

		}

	}

	public static void main(String[] args) throws Exception {


		/*createMockTable();
		new DBApp().createBRINIndex(tblName, "id");
		new DBApp().createBRINIndex(tblName, "name");
		insertValuesIntoTable();*/
		/*new DBApp().createBRINIndex(tblName, "name2");
		testDenseIndex(tblName, "name");
		testBRINIndex(tblName, "name");
		testDenseIndex(tblName, "name2");*/
		
		DBApp app = new DBApp();
		
		/*Page page1 = PageManager.loadPageIfExists("data/" + tblName + "/" + "name2" + "/indices/Dense/page_"
				+ 1 + ".ser");
		Hashtable<String, Object>[] rowsDense = page1.getRows();
		
		for (Hashtable<String, Object> rowDense : rowsDense) {
			Hashtable<String, Object> row = new Hashtable<String, Object>();
			row.put("name2", (String)rowDense.get("value"));
			app.deleteFromTable(tblName, row);
			System.out.println("Done");
		}*/
		
		
		/*for (int i = 1; i <= 20; i++) {
			Hashtable<String, Object> row = new Hashtable<>();
			row.put("id", i);
			app.deleteFromTable(tblName, row);
		}*/
		
		/*Hashtable<String, Object> row = new Hashtable<>();
		row.put("id", -250);
		app.deleteFromTable(tblName, row);*/
		
		/*for (int i = 1; i <= 30; i++) {
			Hashtable<String, Object> row = new Hashtable<>();
			row.put("name2", "Human " + i);
			app.deleteFromTable(tblName, row);
		}*/
		
		/*Hashtable<String, Object> row = new Hashtable<>();
		row.put("name", "NUMNqgyS");
		app.deleteFromTable(tblName, row);*/
		
		/*for (int i = 1; i < 10; i++) {
			Page tempPage = PageManager.loadPageIfExists("data/" + tblName + "/" + "name" + "/indices/Dense/page_"
					+ i + ".ser");
			System.out.println(tempPage);
		}/*
		
		/*Page BRINIndexPage = PageManager.loadPageIfExists(
				"data/" + tblName + "/" + "name2" + "/indices/BRIN/page_" + 1 + ".ser");
		System.out.println(BRINIndexPage);*/
		

	}

	static void testSelection() throws DBAppException {

		String [] Ops = {">" , "<" , ">=" , "<="};
		// test selection
		Object[] objarrValues = new Object[2];
		objarrValues[0] = new Integer(7);
		objarrValues[1] = new Integer(100);
		String[] strarrOperators = new String[2];
		strarrOperators[0] = ">=";
		strarrOperators[1] = "<";

		testSelectionHelper("mockTable0", "id", objarrValues, strarrOperators);
	}

	@SuppressWarnings("rawtypes")
	static void testSelectionHelper(String strTableName, String strTableCol, Object[] objarrValues,
			String[] strarrOperators) throws DBAppException {

		Iterator resultSet = new DBApp().selectFromTable(strTableName, strTableCol, objarrValues, strarrOperators);

		while (resultSet.hasNext())
			System.out.println(resultSet.next());
	}

	static void insertValuesIntoTable() throws DBAppException {
		DBApp app = new DBApp();
		RandomString ranStr = new RandomString(8);
		Hashtable<String, Object> row = new Hashtable<>();
		for (int i = 1; i <= 200; i++) {
			row.put("id", i);
			row.put("name", ranStr.nextString());
			row.put("name2", ranStr.nextString());
			app.insertIntoTable(tblName, row);

		}
		
		/*for (int i = 1; i <= 40; i++) {
			row.put("id", i);
			row.put("name", "Person " + i);
			row.put("name2", "Human " + i);
			app.insertIntoTable(tblName, row);

		}*/

	}

	static void createMockTable() throws DBAppException {
		DBApp app = new DBApp();
		Hashtable<String, String> columns = new Hashtable<String, String>();
		columns.put("id", "java.lang.Integer");
		columns.put("name", "java.lang.String");
		columns.put("name2", "java.lang.String");
		app.createTable(tblName, "id", columns);
	}

}