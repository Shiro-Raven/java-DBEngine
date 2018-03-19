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
			Object oldValue = oldValues.get(colName);
			Object newValue = newValues.get(colName);
			// for every column Load the brin and Go through it with the old
			// value
			int curBRINPage = 1;
			Page BRINPage;
			int densePageNum = 0;
			// Go through the BRIN pages of the current column
			BigLoop: while (true) {
				try {
					BRINPage = IndexUtilities.retrievePage("data/" + strTableName + "/" + colName + "/indices/BRIN",
							curBRINPage);
				} catch (DBAppException e) {
					e.printStackTrace();
					break;
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
			// find the key, check the table and page to avoid duplicate
			// problems
			Page curDensePage = IndexUtilities.retrievePage("data/" + strTableName + "/" + colName + "/indices/Dense",
					densePageNum);
			int i;
			for (i = 0; i < curDensePage.getRows().length; i++) {
				Hashtable<String, Object> curRow = curDensePage.getRows()[i];
				// replace the value
				if (curRow.get("value").equals(oldValue) && (int) curRow.get("pageNumber") == tblPageNum
						&& (int) curRow.get("locInPage") == tupleRowNum) {
					curRow.put("value", newValue);
					break;
				}
			}
			// re arrange
			int direction = ((Comparable) oldValue).compareTo(((Comparable) newValue));
			// false is up, true is down
			boolean goDown = direction < 0 ? true : false;
			ArrayList<Integer> changedIndexPages = new ArrayList<>();
			BigBrotherLoop: while (true) {
				Hashtable<String, Object> curRow = curDensePage.getRows()[i];
				i = goDown ? i++ : i--;
				// Are we at the first entry?
				if (i == -1) {
					// Is this the first dense page?
					if (densePageNum == 1)
						break BigBrotherLoop;
					else {
						// If not, get the previous one
						Page prevPage = IndexUtilities.retrievePage(
								"data/" + strTableName + "/" + colName + "/indices/Dense", densePageNum - 1);
						// and check the last entry
						Hashtable<String, Object> lastRowInPrevPage = prevPage.getRows()[prevPage.getMaxRows() - 1];
						int compareRes = IndexUtilities.compareIndexElements(curRow, lastRowInPrevPage);
						// If they are sorted, return
						if (compareRes < 0)
							break BigBrotherLoop;
						else {
							// If not, swap
							Hashtable<String, Object> tmp = curRow;
							curRow = lastRowInPrevPage;
							lastRowInPrevPage = tmp;
							i = prevPage.getMaxRows() - 1;
							if (!changedIndexPages.contains(densePageNum))
								changedIndexPages.add(densePageNum);
							// and change the dense page and repeat
							PageManager.serializePage(curDensePage, "data/" + strTableName + "/" + colName
									+ "/indices/Dense/page_" + densePageNum-- + ".ser");
							curDensePage = prevPage;
						}
					}
				} // Are we at the last entry?
				else if (i == curDensePage.getMaxRows()) {
					// Try loading a next page
					try {
						Page nextPage = IndexUtilities.retrievePage(
								"data/" + strTableName + "/" + colName + "/indices/Dense", densePageNum + 1);
						// and check the last entry, if successful
						Hashtable<String, Object> firstRowInNextPage = nextPage.getRows()[0];
						int compareRes = IndexUtilities.compareIndexElements(curRow, firstRowInNextPage);
						// If they are sorted, return
						if (compareRes > 0)
							break BigBrotherLoop;
						else {
							// If not, swap
							Hashtable<String, Object> tmp = curRow;
							curRow = firstRowInNextPage;
							firstRowInNextPage = tmp;
							i = 0;
							if (!changedIndexPages.contains(densePageNum))
								changedIndexPages.add(densePageNum);
							// and change the dense page and repeat
							PageManager.serializePage(curDensePage, "data/" + strTableName + "/" + colName
									+ "/indices/Dense/page_" + densePageNum++ + ".ser");
							curDensePage = nextPage;
						}
					} catch (DBAppException e) {
						// If no next page exists, return
						break BigBrotherLoop;
					}
				} else {
					// Else, we are in the middle of a page
					// Get the next/previous element
					Hashtable<String, Object> rowToCompareTo = curDensePage.getRows()[i];
					if (rowToCompareTo == null) {
						if (!changedIndexPages.contains(densePageNum))
							changedIndexPages.add(densePageNum);
						break BigBrotherLoop;
					}
					int compareRes = IndexUtilities.compareIndexElements(curRow, rowToCompareTo);
					// If ordered, stop, else swap
					if (goDown && compareRes < 0 || !goDown && compareRes > 0) {
						if (!changedIndexPages.contains(densePageNum))
							changedIndexPages.add(densePageNum);
						Hashtable<String, Object> tmp = curRow;
						curRow = rowToCompareTo;
						rowToCompareTo = tmp;
					} else if (goDown && compareRes > 0 || !goDown && compareRes < 0) {
						break BigBrotherLoop;
					}
				}
			}
			IndexUtilities.updateBRINIndex(strTableName, colName, changedIndexPages);
		}
	}

}
