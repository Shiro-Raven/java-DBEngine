package team10;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.Set;

public class DeletionUtilities {
	// used when deletion query doesn't include any indexed columns
	public static void deleteTuples(String strTableName, Hashtable<String, Object> htblColNameValue, String primaryKey,
			Set<String> tableKeys) throws IOException {
		int deleteCount = 0; // keeps track of the no. of deleted rows
		int pageNumber = 1;
		Page currentPage = null;

		// load and process table pages one by one until there are no more pages to load
		boolean active = true;
		while (active) {
			try {
				currentPage = PageManager.deserializePage("data/" + strTableName + "/page_" + pageNumber + ".ser");
				// page loaded successfully
				Hashtable<String, Object>[] rows = currentPage.getRows();

				// traversing the page row by row until a null row is encountered
				for (int i = 0; i < currentPage.getMaxRows(); i++) {
					Hashtable<String, Object> row = rows[i];
					if (row == null)
						break;
					boolean match = true;
					boolean quit = false; // quits for loop in case a PK match is found
					// checking for PK match
					if (htblColNameValue.get(primaryKey) != null
							&& htblColNameValue.get(primaryKey).equals(row.get(primaryKey))
							&& !(boolean) row.get("isDeleted")) {
						// current row matches a given primary key value, no more rows can match
						active = false;
						quit = true;
					}
					// checking current row against the given tuple
					for (String tableKey : tableKeys) {
						if (htblColNameValue.get(tableKey) != null
								&& !htblColNameValue.get(tableKey).equals(row.get(tableKey))) {
							match = false;
							break;
						}
					}

					if (match && !(boolean) row.get("isDeleted")) {
						// current row matches the given tuple, proceed in deleting it
						row.put("isDeleted", true);
						deleteCount++;
					}

					if (quit)
						break;
				}

				PageManager.serializePage(currentPage,
						"data/" + strTableName + "/page_" + currentPage.getPageNumber() + ".ser");
				pageNumber++;

			} catch (FileNotFoundException e) {
				// there are no more pages in this table
				active = false; // break out of the while loop to finalize
			} catch (Exception e) {
				// unexpected exception
				e.printStackTrace();
			}
		}
		// finalization
		System.out.printf("%d row(s) deleted\n", deleteCount);
	}

	// used when deletion query includes an indexed column
	public static void deleteTuplesIndexed(String strTableName, Hashtable<String, Object> htblColNameValue,
			String primaryKey, LinkedList<String> indexed_columns, Set<String> tableKeys) throws IOException {
		int deleteCount = 0; // keeps track of the no. of deleted rows
		Page pageBRIN = null;
		int pageNumberBRIN = 1;
		ArrayList<Integer> modifiedBRIN = new ArrayList<Integer>();

		// check for PK in given tuple
		if (htblColNameValue.get(primaryKey) != null && indexed_columns.contains(primaryKey)) {
			// primary key provided and indexed
			boolean active = true;
			// load and process BRIN index pages one by one until there are no more pages
			while (true) {
				try {
					// retrieve the index page
					pageBRIN = PageManager.deserializePage("data/" + strTableName + "/" + primaryKey + "/indices/BRIN/"
							+ "page_" + pageNumberBRIN + ".ser");

					@SuppressWarnings("rawtypes")
					int positionInIndex = searchBRIN(pageBRIN.getRows(), (Comparable) htblColNameValue.get(primaryKey),
							primaryKey);

					// if not within any range in this index page, load the next page
					if (positionInIndex == -1) {
						pageNumberBRIN++;
						continue;
					}

					// if the target was less than the current minimum then it doesn't exist
					else if (positionInIndex == -2)
						break;

					// if target is within a range in this index page
					else {
						// add BRIN page number to modified BRIN pages ArrayList
						modifiedBRIN.add(pageNumberBRIN);

						// get pageNumber
						int pageNumber;
						pageNumber = (int) pageBRIN.getRows()[positionInIndex].get("pageNumber");

						// load page
						Page pageToDeleteFrom = null;
						pageToDeleteFrom = PageManager
								.deserializePage("data/" + strTableName + "/" + "page_" + pageNumber + ".ser");
						Hashtable<String, Object>[] rows = pageToDeleteFrom.getRows();

						// traversing the page row by row until a null row is encountered
						for (int i = 0; i < pageToDeleteFrom.getMaxRows(); i++) {
							Hashtable<String, Object> row = rows[i];

							// end of page
							if (row == null)
								break;

							// checking for PK match
							if (htblColNameValue.get(primaryKey).equals(row.get(primaryKey))
									&& !(boolean) row.get("isDeleted")) {
								// current row matches a given primary key value, no more rows can match
								row.put("isDeleted", true);
								deleteCount++;
								active = false;
							}

						}

						PageManager.serializePage(pageToDeleteFrom,
								"data/" + strTableName + "/page_" + pageToDeleteFrom.getPageNumber() + ".ser");

					}
				} catch (FileNotFoundException e) {
					// there are no more pages in this table
					active = false; // break out of the while loop to finalize
				} catch (Exception e) {
					// unexpected exception
					e.printStackTrace();
				}

				if (!active)
					break;
			}
		} else {
			// primary key either not provided or not indexed
			// choose an indexed column to use
			String indexedColumn = indexed_columns.getFirst();
			boolean active = true;
			// load and process BRIN index pages one by one until there are no more pages
			while (true) {
				try {
					// retrieve the index page
					pageBRIN = PageManager.deserializePage("data/" + strTableName + "/" + indexedColumn
							+ "/indices/BRIN/" + "page_" + pageNumberBRIN + ".ser");

					@SuppressWarnings("rawtypes")
					int positionInIndex = searchBRIN(pageBRIN.getRows(),
							(Comparable) htblColNameValue.get(indexedColumn), indexedColumn);

					// if not within any range in this index page, load the next page
					if (positionInIndex == -1) {
						pageNumberBRIN++;
						continue;
					}

					// the target was smaller than the min of a page it reached so it doesn't exist
					else if (positionInIndex == -2)
						break;

					// if target is within a range in this index page
					else {
						// add BRIN page number to modified BRIN pages ArrayList
						modifiedBRIN.add(pageNumberBRIN);

						// get pageNumber
						int pageNumberDense = (int) pageBRIN.getRows()[positionInIndex].get("pageNumber");

						// load Dense index page
						Page pageDense = null;
						pageDense = PageManager.deserializePage("data/" + strTableName + "/" + indexedColumn
								+ "/indices/Dense/" + "page_" + pageNumberDense + ".ser");
						Hashtable<String, Object>[] rowsDense = pageDense.getRows();

						// traversing the Dense index page row by row until a null row is encountered
						for (int i = 0; i < pageDense.getMaxRows(); i++) {
							Hashtable<String, Object> rowDense = rowsDense[i];

							// end of page
							if (rowDense == null)
								break;

							// checking for dense index match
							if (rowDense.get(indexedColumn).equals(htblColNameValue.get(indexedColumn))
									&& !(boolean) rowDense.get("isDeleted")) {
								// current row matches the given tuple, proceed in deleting it
								// delete all Dense consecutive duplicates that match the query
								for (int j = i; j < pageDense.getMaxRows(); j++) {
									rowDense = rowsDense[j];
									if (!rowDense.get(indexedColumn).equals(htblColNameValue.get(indexedColumn)))
										break;
									// get pageNumber and locInPage
									int pageNumber = (int) rowDense.get("pageNumber");
									int locInPage = (int) rowDense.get("locInPage");
									// load page
									Page pageToDeleteFrom = null;
									pageToDeleteFrom = PageManager.deserializePage(
											"data/" + strTableName + "/" + "page_" + pageNumber + ".ser");
									Hashtable<String, Object> row = pageToDeleteFrom.getRows()[locInPage];
									// check if row matches the given tuple
									boolean match = true;
									for (String tableKey : tableKeys) {
										if (htblColNameValue.get(tableKey) != null
												&& !htblColNameValue.get(tableKey).equals(row.get(tableKey))) {
											match = false;
											break;
										}
									}
									if (match && !(boolean) row.get("isDeleted")) {
										rowDense.put("isDeleted", true);
										row.put("isDeleted", true);
										PageManager.serializePage(pageToDeleteFrom, "data/" + strTableName + "/"
												+ "page_" + pageToDeleteFrom.getPageNumber() + ".ser");
										deleteCount++;
										// checking for PK match
										if (htblColNameValue.get(primaryKey) != null
												&& htblColNameValue.get(primaryKey).equals(row.get(primaryKey))) {
											// current row matches a given primary key value, no more rows can match
											active = false;
											break;
										}
									}
								}
								break;
							}
						}

						PageManager.serializePage(pageDense, "data/" + strTableName + "/" + indexedColumn
								+ "/indices/Dense/" + "page_" + pageDense.getPageNumber() + ".ser");

					}
				} catch (FileNotFoundException e) {
					// there are no more pages in this table
					active = false; // break out of the while loop to finalize
				} catch (Exception e) {
					// unexpected exception
					e.printStackTrace();
				}

				if (!active)
					break;
			}
		}

		// TODO pass modifiedBRIN to updateBRIN() method

		// finalization
		System.out.printf("%d row(s) deleted\n", deleteCount);
	}

	// adapted from InsertionUtilities.searchIndex()
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static int searchBRIN(Hashtable<String, Object>[] rows, Comparable target, String indexedKey) {
		for (int i = 0; i < rows.length; i++) {

			// the index's page is not fully empty and we reached the end of its
			// entries
			// I assume that there are no fully empty index pages
			if (rows[i] == null)
				return -1;

			// check if the index row is deleted
			if ((boolean) rows[i].get("isDeleted"))
				continue;

			Comparable currentRowMax = (Comparable) rows[i].get(indexedKey + "Max");
			Comparable currentRowMin = (Comparable) rows[i].get(indexedKey + "Min");

			// if the target belongs in the range
			if (target.compareTo(currentRowMax) <= 0 && target.compareTo(currentRowMin) >= 0) {
				return i;
			}

			// if the target is below the range
			if (target.compareTo(currentRowMin) < 0) {
				return -2;
			}
		}
		// no place found
		return -1;
	}
}
