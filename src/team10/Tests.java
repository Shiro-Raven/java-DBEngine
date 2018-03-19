package team10;

import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Random;

public class Tests {

	static void testIndex(String tblName, String colName) {

		int denseIndexPageNumber = 1;
		Object lastValue = '\n';

		while (true) {

			Page denseIndexPage = loadPage(
					"data/" + tblName + "/" + colName + "/indices/Dense/page_" + denseIndexPageNumber++ + ".ser");

			if (denseIndexPage == null) {

				System.out.println("Check Is Complete!");
				return;

			}

			// Looping Through Rows Of EACH Index File
			for (int i = 0; i < denseIndexPage.getMaxRows() && denseIndexPage.getRows()[i] != null; i++) {

				Hashtable<String, Object> denseIndexRow = denseIndexPage.getRows()[i];

				Page tablePage = loadPage(
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

	static Page loadPage(String filepath) {

		try {

			return PageManager.deserializePage(filepath);

		} catch (Exception e) {

			return null;

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

		// IndexUtilities.createDenseIndex("idTest", "first_name");
		// testIndex("idTest", "first_name");

	}

}