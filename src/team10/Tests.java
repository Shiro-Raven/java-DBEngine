package team10;

import java.io.IOException;
import java.text.ParseException;
import java.util.Hashtable;
import java.util.Iterator;

public class Tests {

	static String tblName = "muccTable";

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

	static void testSelection() throws DBAppException {

		Object[] objarrValues = { new Integer(20), new Integer(20) };
		String[] strarrOperators = { new String(">"), new String("<") };

		@SuppressWarnings("rawtypes")
		Iterator resultSet = new DBApp().selectFromTable(tblName, "id", objarrValues, strarrOperators);

		while (resultSet.hasNext())
			System.out.println(resultSet.next());

	}

	static void deleteValuesFromTable() throws DBAppException, ClassNotFoundException, ParseException, IOException {

		DBApp app = new DBApp();
		Hashtable<String, Object> row = new Hashtable<>();
		for (int i = 150; i <= 200; i++) {
			row.put("id", i);
			app.deleteFromTable(tblName, row);
		}

	}

	static void updateValuesIntoTable() throws DBAppException, ClassNotFoundException, ParseException, IOException {

		DBApp app = new DBApp();
		RandomString ranStr = new RandomString(8);
		Hashtable<String, Object> row = new Hashtable<>();
		for (int i = 50; i <= 100; i++) {
			row.put("name", ranStr.nextString());
			row.put("name2", ranStr.nextString());
			app.updateTable(tblName, i + "", row);
		}

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

	}

	static void createMockTable() throws DBAppException {
		DBApp app = new DBApp();
		Hashtable<String, String> columns = new Hashtable<String, String>();
		columns.put("id", "java.lang.Integer");
		columns.put("name", "java.lang.String");
		columns.put("name2", "java.lang.String");
		app.createTable(tblName, "id", columns);
	}

	public static void main(String[] args) throws Exception {

		DBApp app = new DBApp();
		createMockTable();
		app.createBRINIndex(tblName, "id");
		app.createBRINIndex(tblName, "name");
		insertValuesIntoTable();
		app.createBRINIndex(tblName, "name2");
		testDenseIndex(tblName, "id");
		testDenseIndex(tblName, "name");
		testBRINIndex(tblName, "name");
		testDenseIndex(tblName, "name2");
		testBRINIndex(tblName, "name2");

		testSelection();

		updateValuesIntoTable();
		testDenseIndex(tblName, "id");
		testDenseIndex(tblName, "name");
		testBRINIndex(tblName, "name");
		testDenseIndex(tblName, "name2");
		testBRINIndex(tblName, "name2");
		
		deleteValuesFromTable();
		testDenseIndex(tblName, "id");
		testDenseIndex(tblName, "name");
		testBRINIndex(tblName, "name");
		testDenseIndex(tblName, "name2");
		testBRINIndex(tblName, "name2");

	}

}