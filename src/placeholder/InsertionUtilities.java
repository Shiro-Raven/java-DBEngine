package placeholder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.Hashtable;
import java.util.Set;

/**
 * 
 * @author Omar Assumptions: When Deleting, we don't set the values in the
 *         hashtable to null. We should instead mark them somehow with an extra
 *         variable, indicating that they were deleted. Otherwise, the search
 *         will not produce right results.
 *
 */
public class InsertionUtilities {
	// change exception to DBAppException

	@SuppressWarnings("rawtypes")
	public static int[] searchForInsertionPosition(String strTableName, ArrayList<String> primaryKey,
			Hashtable<String, Object> htblColNameValue) throws Exception {

		int pageNumber = 1;
		int rowNumber = 0;

		Page currentPage = null;

		// declare array lists for primary key comparisons
		ArrayList<Comparable> insertTupleKeyValues = null;
		ArrayList<Comparable> tableRowKeyValues = null;

		// get the values of the primary key of the tuple we want to insert
		// for future comparisons
		insertTupleKeyValues = getPrimaryKeyValues(primaryKey, htblColNameValue);

		// initialize the incremental key for comparison
		ArrayList<Comparable> incrementalKeyValues = new ArrayList<Comparable>();
		incrementalKeyValues.add(insertTupleKeyValues.get(0));
		int incrementalKeyCounter = 1;

		while (true) {
			try {
				currentPage = PageManager.deserializePage("data/" + strTableName + "/" + "page_" + pageNumber + ".ser");
			} catch (Exception e) {
				// There are no more pages for this table.

				// There are no pages at all
				if (currentPage == null) {
					return new int[] { 1, 0 };
				}

				// the last requested page was not found
				else {
					// load the rows of the previous page
					Hashtable<String, Object>[] table = currentPage.getRows();

					// begin the iteration on the rows
					for (int i = 0; i < currentPage.getMaxRows(); i++) {
						// The table row is empty
						if (table[i] == null) {
							rowNumber = i;
							return new int[] { currentPage.getPageNumber(), rowNumber };
						}
						// The table row is not empty
						Hashtable<String, Object> currentRow = table[i];

						tableRowKeyValues = getPrimaryKeyValues(primaryKey, currentRow);

						int comparisonResult = compareKeys(incrementalKeyValues, tableRowKeyValues);

						// The values from incementalKey and tableRowKeyValues
						// are exactly alike
						// Therefore, the key must be expanded

						while (comparisonResult == 0) {
							if (incrementalKeyValues.size() == tableRowKeyValues.size()) {
								throw new Exception("The primary key constraint was violated.");
							} else {
								// add one more column to the primary key
								// columns examined
								// e.g. firstName -> firstName, lastName
								incrementalKeyValues.add(insertTupleKeyValues.get(incrementalKeyCounter));
								incrementalKeyCounter++;
								// see if the current row is now okay to
								// insert the values in
								comparisonResult = compareKeys(incrementalKeyValues, tableRowKeyValues);
							}
						}
						// comparison result is no longer 0

						// The values from incrementalKey are less than
						// tableRowKeyValues
						if (comparisonResult == -1) {
							rowNumber = i;
							return new int[] { currentPage.getPageNumber(), rowNumber };
						}
						// The values from incrementalKey are not less than or
						// equal tableRowKeyValues
						// Therefore, we continue the iteration, checking the
						// next row
					}
					// We went through all the rows of the page and no suitable
					// location was found
					// Return the page number of a page to be newly created
					return new int[] { currentPage.getPageNumber() + 1, 0 };
				}
			}

			// The catch block ends here
			// The page was loaded successfully and is now stored in currentPage

			// load the first row of the page
			Hashtable<String, Object> firstRow = currentPage.getRows()[0];

			// for some reason, the firstRow was null
			if (firstRow == null) {
				return new int[] { currentPage.getPageNumber(), 0 };
			}

			// get the first row's key values
			tableRowKeyValues = getPrimaryKeyValues(primaryKey, firstRow);

			// compare
			int comparisonResult = compareKeys(incrementalKeyValues, tableRowKeyValues);

			// The values from incementalKey and tableRowKeyValues
			// are exactly alike
			// Therefore, the key must be expanded

			while (comparisonResult == 0) {
				if (incrementalKeyValues.size() == tableRowKeyValues.size()) {
					throw new Exception("The primary key constraint was violated.");
				} else {
					// add one more column to the primary key
					// columns examined
					// e.g. firstName -> firstName, lastName
					incrementalKeyValues.add(insertTupleKeyValues.get(incrementalKeyCounter));
					incrementalKeyCounter++;
					// see if the current row is now okay to
					// insert the values in
					comparisonResult = compareKeys(incrementalKeyValues, tableRowKeyValues);
				}
			}
			// comparison result is no longer 0

			// The values from incrementalKey are less than
			// tableRowKeyValues
			if (comparisonResult == -1) {
				break;
			} else {
				pageNumber++;
			}
		}

		// A page where the first element is greater than the tuple to be
		// inserted has been found
		// Proceed to the page before it and search linearly
		try {
			currentPage = PageManager.deserializePage(
					"data/" + strTableName + "/" + "page_" + (currentPage.getPageNumber() - 1) + ".ser");
		} catch (Exception e) {
			// unexpected error
			// could not load the previous page
			// e.printStackTrace();
		}

		// re-initialize the incremental key for comparison
		incrementalKeyValues = new ArrayList<Comparable>();
		incrementalKeyValues.add(insertTupleKeyValues.get(0));
		incrementalKeyCounter = 1;

		// load the rows of the previous page
		Hashtable<String, Object>[] table = currentPage.getRows();

		// begin the iteration on the rows
		for (int i = 0; i < currentPage.getMaxRows(); i++) {
			// The table row is empty
			if (table[i] == null) {
				rowNumber = i;
				return new int[] { currentPage.getPageNumber(), rowNumber };
			}

			// The table row is not empty
			Hashtable<String, Object> currentRow = table[i];

			tableRowKeyValues = getPrimaryKeyValues(primaryKey, currentRow);

			int comparisonResult = compareKeys(incrementalKeyValues, tableRowKeyValues);

			// The values from incementalKey and tableRowKeyValues
			// are exactly alike
			// Therefore, the key must be expanded

			while (comparisonResult == 0) {
				// is the key expandable?
				if (incrementalKeyValues.size() == tableRowKeyValues.size()) {
					throw new Exception("The primary key constraint was violated.");
				} else {
					// add one more column to the primary key
					// columns examined
					// e.g. for P.K.=(firstName,lastName) we expand: firstName
					// -> firstName, lastName
					incrementalKeyValues.add(insertTupleKeyValues.get(incrementalKeyCounter));
					incrementalKeyCounter++;
					// see if the current row is now okay to
					// insert the values in
					comparisonResult = compareKeys(incrementalKeyValues, tableRowKeyValues);
				}
			}
			// comparison result is no longer 0

			// The values from incrementalKey are less than
			// tableRowKeyValues
			if (comparisonResult == -1) {
				rowNumber = i;
				return new int[] { currentPage.getPageNumber(), rowNumber };
			}
		}

		// The whole page contained values less than the ones we want to insert
		// Therefore, we insert in the first position of the next page
		return new int[] { currentPage.getPageNumber() + 1, 0 };
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	private static int compareKeys(ArrayList<Comparable> incrementalKey, ArrayList<Comparable> tableRowKeyValues) {

		for (int i = 0; i < incrementalKey.size(); i++) {
			// compare corresponding keys in the two lists
			int result = incrementalKey.get(i).compareTo(tableRowKeyValues.get(i));

			// if any of the incrementalKeys examined sequentially
			// is less than the corresponding tableRowKey
			// return -1
			if (result == -1) {
				return -1;
			}

			// if any of the incrementalKeys examined sequentially
			// is greater than the corresponding tableRowKey
			// return 1
			if (result == 1) {
				return 1;
			}
		}

		// all the keys are equal, so in this case return 0
		return 0;
	}

	@SuppressWarnings("rawtypes")
	private static ArrayList<Comparable> getPrimaryKeyValues(ArrayList<String> primaryKey,
			Hashtable<String, Object> tuple) {

		ArrayList<Comparable> tupleKeyValues = new ArrayList<Comparable>();

		for (int i = 0; i < primaryKey.size(); i++) {
			tupleKeyValues.add((Comparable) tuple.get(primaryKey.get(i)));
		}

		return tupleKeyValues;
	}

	public static boolean isValidTuple(Hashtable<String, String> ColNameType, Hashtable<String, Object> Tuple) {
		Set<String> keys = Tuple.keySet();
		for (String key : keys) {
			if (!ColNameType.get(key).equals(Tuple.get(key).getClass().toString().substring(6)))
				return false;
		}
		return true;
	}

	public static boolean insertIntoTuple(String tableName, int[] positionToInsertAt,
			Hashtable<String, Object> htblColNameValue) throws IOException {

		Page page = InsertionUtilities.loadPage(tableName, positionToInsertAt[0]);
		int maxRows = PageManager.getMaximumRowsCountinPage();
		htblColNameValue.put("TouchDate", new Date());
		Hashtable<String, Object> tempHtblColNameValue;

		for (int i = positionToInsertAt[1]; i < maxRows; i++) {

			tempHtblColNameValue = page.getRows()[i];
			page.getRows()[i] = htblColNameValue;
			htblColNameValue = tempHtblColNameValue;

			if (htblColNameValue == null)
				break;

			if (i == maxRows - 1) {

				i = -1; // Reset i
				PageManager.serializePage(page, "data/" + tableName + "/" + positionToInsertAt[0] + ".ser");
				page = InsertionUtilities.loadPage(tableName, ++positionToInsertAt[0]);

			}

		}

		PageManager.serializePage(page, "data/" + tableName + "/" + positionToInsertAt[0] + ".ser");
		return true;

	}

	public static Page loadPage(String tableName, int pageNumber) throws IOException {

		Page page;

		try {

			page = PageManager.deserializePage("data/" + tableName + "/page_" + pageNumber + ".ser");

		} catch (Exception e) {

			page = new Page(pageNumber);

		}

		return page;

	}

}