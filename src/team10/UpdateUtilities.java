package team10;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Hashtable;

public class UpdateUtilities {

	// Here I will be returning an Arraylist of two things: the Hashtable, and
	// the Primary Key column name
	public static ArrayList<Object> getColumnsAndKey(String strTableName) {
		String line = null;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader("data/metadata.csv"));
			line = br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}

		String PKey = null;
		ArrayList<String> indexedColumns = new ArrayList<>();
		Hashtable<String, String> ColNameType = new Hashtable<>();

		while (line != null) {
			String[] content = line.split(",");

			if (content[0].equals(strTableName)) {
				ColNameType.put(content[1], content[2]);
				if ((content[3].toLowerCase()).equals("true"))
					PKey = content[1];
				if ((content[4].toLowerCase()).equals("true"))
					indexedColumns.add(content[1]);
			}
			try {
				line = br.readLine();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}

		try {
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}

		ArrayList<Object> Data = new ArrayList<>();
		Data.add(ColNameType);
		Data.add(PKey);
		Data.add(indexedColumns);

		return Data;
	}

	public static boolean checkNotUsed(String tableName, Object newValue, String PKName) {
		int pageNum = 1;
		while (true) {
			try {
				Page curPage = PageManager.deserializePage("data/" + tableName + "/" + "page_" + pageNum + ".ser");
				for (int i = 0; i < curPage.getRows().length; i++) {
					Hashtable<String, Object> curRow = curPage.getRows()[i];
					// if a matching row is found
					if (curRow.get(PKName).equals(newValue) && !((boolean) curRow.get("isDeleted"))) {
						return false;
					}
				}
			} catch (Exception e) {
				return true;
			}
			pageNum++;
		}
	}

	public static Object getTypedPKey(String keyType, String strKey) throws ParseException {
		Object keyValue = null;
		switch (keyType) {
		case "java.lang.Integer":
			keyValue = Integer.parseInt(strKey);
			break;
		case "java.lang.String":
			keyValue = strKey;
			break;
		case "java.lang.Double":
			keyValue = Double.parseDouble(strKey);
			break;
		case "java.util.Date":
			keyValue = new SimpleDateFormat("dd/MM/yyyy").parse(strKey);
			break;
		}
		return keyValue;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public static void updateDenseIndex(String strTableName, Hashtable<String, Object> newValues,
			Hashtable<String, Object> oldValues, int tblPageNum, int tupleRowNum) throws DBAppException, IOException {
		for (String colName : oldValues.keySet()) {
			System.out.println("Now rearranging dense of " + colName);
			Object oldValue = oldValues.get(colName);
			Object newValue = newValues.get(colName);
			// for every column Load the brin and Go through it with the old
			// value
			int curBRINPage = 1;
			Page BRINPage = null;
			int densePageNum = 0;
			// Go through the BRIN pages of the current column
			BigLoop: while (true) {
				try {
					BRINPage = IndexUtilities.retrievePage("data/" + strTableName + "/" + colName + "/indices/BRIN",
							curBRINPage);
				} catch (DBAppException e) {
					e.printStackTrace();
				}

				// For every BRIN page, find the corresponding dense index page
				// of the old value
				Comparable target = (Comparable) oldValues.get(colName);
				for (int i = 0; i < BRINPage.getRows().length; i++) {
					Comparable currentRowMax = (Comparable) BRINPage.getRows()[i].get(colName + "Max");
					Comparable currentRowMin = (Comparable) BRINPage.getRows()[i].get(colName + "Min");

					if (target.compareTo(currentRowMax) <= 0 && target.compareTo(currentRowMin) >= 0) {
						densePageNum = (int) BRINPage.getRows()[i].get("pageNumber");
						break BigLoop;
					}
				}
				curBRINPage++;
			}
			System.out.println("Found Dense Index page number. value is " + densePageNum);
			// find the key, check the table and page to avoid duplicate
			// problems
			Page curDensePage = IndexUtilities.retrievePage("data/" + strTableName + "/" + colName + "/indices/Dense",
					densePageNum);
			int curRowIndex;
			Hashtable<String, Object> curRow = null;
			for (curRowIndex = 0; curRowIndex < curDensePage.getRows().length; curRowIndex++) {
				curRow = curDensePage.getRows()[curRowIndex];
				// replace the value
				if (curRow.get("value").equals(oldValue) && (int) curRow.get("pageNumber") == tblPageNum
						&& (int) curRow.get("locInPage") == tupleRowNum) {
					System.out.println("New value added");
					curDensePage.getRows()[curRowIndex].put("value", newValue);
					System.out.println(curDensePage.getRows()[curRowIndex].toString());
					break;
				}
			}
			System.out.println("New value added. i is " + curRowIndex);
			// re arrange
			int direction = ((Comparable) oldValue).compareTo(((Comparable) newValue));
			// If the value didn't really change, continue
			System.out.println("Direction is " + direction);
			if (direction == 0)
				continue;
			// false is up, true is down
			boolean goDown = direction < 0 ? true : false;
			ArrayList<Integer> changedIndexPages = new ArrayList<>();
			int newRowIndex;
			System.out.println("Entered BigLoop");
			BigBrotherLoop: while (true) {
				System.out.println("Old i is " + curRowIndex);
				newRowIndex = goDown ? curRowIndex + 1 : curRowIndex - 1;
				System.out.println("New i is " + newRowIndex);
				// Are we at the first entry?
				if (newRowIndex == -1) {
					System.out.println("First Entry case");
					// Is this the first dense page?
					if (densePageNum == 1) {
						if (!changedIndexPages.contains(densePageNum))
							changedIndexPages.add(densePageNum);
						PageManager.serializePage(curDensePage, "data/" + strTableName + "/" + colName
								+ "/indices/Dense/page_" + densePageNum + ".ser");
						break BigBrotherLoop;
					} else {
						// If not, get the previous one
						Page prevPage = IndexUtilities.retrievePage(
								"data/" + strTableName + "/" + colName + "/indices/Dense", densePageNum - 1);
						// and check the last entry
						int compareRes = IndexUtilities.compareIndexElements(curDensePage.getRows()[curRowIndex],
								prevPage.getRows()[prevPage.getMaxRows() - 1]);
						// If they are sorted, return
						if (compareRes < 0) {
							PageManager.serializePage(curDensePage, "data/" + strTableName + "/" + colName
									+ "/indices/Dense/page_" + densePageNum + ".ser");
							break BigBrotherLoop;
						} else {

							// If not, swap
							Hashtable<String, Object> tmp = curDensePage.getRows()[curRowIndex];
							curDensePage.getRows()[curRowIndex] = prevPage.getRows()[prevPage.getMaxRows() - 1];
							prevPage.getRows()[prevPage.getMaxRows() - 1] = tmp;
							curRowIndex = prevPage.getMaxRows() - 1;
							System.out.println("Swapped and went to previous page, i is " + curRowIndex);
							if (!changedIndexPages.contains(densePageNum))
								changedIndexPages.add(densePageNum);
							// and change the dense page and repeat
							PageManager.serializePage(curDensePage, "data/" + strTableName + "/" + colName
									+ "/indices/Dense/page_" + densePageNum-- + ".ser");
							curDensePage = prevPage;
						}
					}
				} // Are we at the last entry?
				else if (newRowIndex == curDensePage.getMaxRows()) {
					System.out.println("Last Entry case");
					// Try loading a next page
					try {
						Page nextPage = IndexUtilities.retrievePage(
								"data/" + strTableName + "/" + colName + "/indices/Dense", densePageNum + 1);
						// and check the last entry, if successful
						int compareRes = IndexUtilities.compareIndexElements(curDensePage.getRows()[curRowIndex],
								nextPage.getRows()[0]);
						// If they are sorted, return
						if (compareRes > 0) {
							PageManager.serializePage(curDensePage, "data/" + strTableName + "/" + colName
									+ "/indices/Dense/page_" + densePageNum + ".ser");
							break BigBrotherLoop;
						} else {
							System.out.println("Swapped and went to next page, i is " + curRowIndex);
							// If not, swap
							Hashtable<String, Object> tmp = curDensePage.getRows()[curRowIndex];
							curDensePage.getRows()[curRowIndex] = nextPage.getRows()[0];
							nextPage.getRows()[0] = tmp;
							curRowIndex = 0;
							if (!changedIndexPages.contains(densePageNum))
								changedIndexPages.add(densePageNum);
							// and change the dense page and repeat
							PageManager.serializePage(curDensePage, "data/" + strTableName + "/" + colName
									+ "/indices/Dense/page_" + densePageNum++ + ".ser");
							curDensePage = nextPage;
						}
					} catch (DBAppException e) {
						if (!changedIndexPages.contains(densePageNum))
							changedIndexPages.add(densePageNum);
						PageManager.serializePage(curDensePage, "data/" + strTableName + "/" + colName
								+ "/indices/Dense/page_" + densePageNum + ".ser");
						// If no next page exists, return
						break BigBrotherLoop;
					}
				} else {
					System.out.println("middle of page case");
					// Else, we are in the middle of a page
					// Get the next/previous element;
					if (curDensePage.getRows()[newRowIndex] == null) {
						if (!changedIndexPages.contains(densePageNum))
							changedIndexPages.add(densePageNum);
						PageManager.serializePage(curDensePage, "data/" + strTableName + "/" + colName
								+ "/indices/Dense/page_" + densePageNum + ".ser");
						break BigBrotherLoop;
					}
					int compareRes = IndexUtilities.compareIndexElements(curDensePage.getRows()[curRowIndex],
							curDensePage.getRows()[newRowIndex]);
					System.out.println(curDensePage.getRows()[curRowIndex].toString());
					System.out.println(curDensePage.getRows()[newRowIndex].toString());
					System.out.println("comparison returned " + compareRes);

					// If ordered, stop, else swap
					if (goDown && compareRes < 0 || !goDown && compareRes > 0) {
						if (!changedIndexPages.contains(densePageNum))
							changedIndexPages.add(densePageNum);
						System.out.println("Swapped within the page, i is " + curRowIndex);
						Hashtable<String, Object> tmp = curDensePage.getRows()[curRowIndex];
						curDensePage.getRows()[curRowIndex] = curDensePage.getRows()[newRowIndex];
						curDensePage.getRows()[newRowIndex] = tmp;
						curRowIndex = newRowIndex;
					} else if (goDown && compareRes > 0 || !goDown && compareRes < 0) {
						if (!changedIndexPages.contains(densePageNum))
							changedIndexPages.add(densePageNum);
						PageManager.serializePage(curDensePage, "data/" + strTableName + "/" + colName
								+ "/indices/Dense/page_" + densePageNum + ".ser");
						break BigBrotherLoop;
					} else {
						System.out.println("Something is Wrong");
						return;
					}
					curRowIndex = newRowIndex;
				}
			}
			System.out.println("Finished re-arranging, calling Reda's method");
			IndexUtilities.updateBRINIndexOnDense(strTableName, colName, changedIndexPages);
		}
	}

}
