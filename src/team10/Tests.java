package team10;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Random;

public class Tests {

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
				Page tempPage = PageManager.loadPageIfExists(
						"data/" + tblName + "/page_" + ((int) BRINIndexRow.get("pageNumber")) + ".ser");

				int lastIndex;
				for (lastIndex = -1; lastIndex < (tempPage.getMaxRows() - 1)
						&& tempPage.getRows()[lastIndex + 1] != null; lastIndex++)
					;

				if (!tempPage.getRows()[0].get(colName).equals(BRINIndexRow.get(colName + "Min")))
					System.out.println("Error With Initial Values: " + BRINIndexRow + " & " + tempPage.getRows()[0]);
				if (!tempPage.getRows()[lastIndex].get(colName).equals(BRINIndexRow.get(colName + "Max")))
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

		DBApp app = new DBApp();
		ArrayList<Integer> usedId = new ArrayList<Integer>();
		Random ranInt = new Random(Integer.MAX_VALUE);
		RandomString ranStr = new RandomString(8);

		Hashtable<String, String> hRow = new Hashtable<>();
		hRow.put("id", Integer.class.getName());
		hRow.put("first_name", String.class.getName());
		app.createTable("idTest", "id", hRow);

		for (int i = 0; i < 200; i++) {

			Hashtable<String, Object> row = new Hashtable<>();

			int id;
			do {
				id = ranInt.nextInt();
			} while (usedId.contains(id));
			usedId.add(id);

			row.put("id", id);
			row.put("first_name", ranStr.nextString());

			app.insertIntoTable("idTest", row);

		}

		IndexUtilities.createDenseIndex("idTest", "first_name");

		testDenseIndex("idTest", "first_name");
		testBRINIndex("idTest", "first_name");

	}

}