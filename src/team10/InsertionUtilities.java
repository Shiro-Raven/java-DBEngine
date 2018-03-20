package team10;

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

// important!!!!: Not all PageManager.deserialize can be replaced with
// loadPage!!! And so is the case with indices.
public class InsertionUtilities {

	public static int[] searchForInsertionPosition(String strTableName, String primaryKey,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {
		int pageNumber = 1;
		Page currentPage = null;

		while (true) {
			try {
				currentPage = PageManager.deserializePage("data/" + strTableName + "/" + "page_" + pageNumber + ".ser");
			} catch (IOException | ClassNotFoundException e) {
				// we need a new page
				if (currentPage == null)
					return new int[] { 1, 0 };
				else
					return new int[] { currentPage.getPageNumber() + 1, 0 };
			}

			// for some reason, the firstRow was null
			if (currentPage.getRows()[0] == null) {
				return new int[] { currentPage.getPageNumber(), 0 };
			}

			// check if we can insert in this page
			int rowNumber = getRowPositionToInsertAt(currentPage, primaryKey, htblColNameValue);

			if (rowNumber == -1) {
				pageNumber++;
				continue;
			} else {
				return new int[] { currentPage.getPageNumber(), rowNumber };
			}
		}
	}

	public static boolean isValidTuple(Hashtable<String, String> ColNameType, Hashtable<String, Object> Tuple) {
		Set<String> keys = Tuple.keySet();
		for (String key : keys) {
			if (!(ColNameType.get(key) == null)
					&& !ColNameType.get(key).equals(Tuple.get(key).getClass().toString().substring(6)))
				return false;
		}
		return true;
	}

	public static boolean insertTuple(String tableName, int[] positionToInsertAt,
			Hashtable<String, Object> htblColNameValue, boolean isNew) throws IOException {

		Page page = InsertionUtilities.loadPage(tableName, positionToInsertAt[0]);
		int maxRows = PageManager.getMaximumRowsCountinPage();
		if (isNew) {

			htblColNameValue.put("TouchDate", new Date());
			htblColNameValue.put("isDeleted", false);

		}
		Hashtable<String, Object> tempHtblColNameValue;

		for (int i = positionToInsertAt[1]; i < maxRows; i++) {

			tempHtblColNameValue = page.getRows()[i];
			page.getRows()[i] = htblColNameValue;
			htblColNameValue = tempHtblColNameValue;

			// removed isDeleted == true

			if (htblColNameValue == null)
				break;

			if (i == maxRows - 1) {

				i = -1; // Reset i
				PageManager.serializePage(page, "data/" + tableName + "/page_" + positionToInsertAt[0] + ".ser");
				page = InsertionUtilities.loadPage(tableName, ++positionToInsertAt[0]);

			}

		}

		PageManager.serializePage(page, "data/" + tableName + "/page_" + positionToInsertAt[0] + ".ser");
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

	@SuppressWarnings("rawtypes")
	public static int[] searchForInsertionPositionIndexed(String strTableName, String primaryKey,
			Hashtable<String, Object> htblColNameValue) throws DBAppException {

		Page indexPage = null;
		int indexPageNumber = 1;

		// search through the index
		while (true) {
			try {
				// retrieve the index page
				indexPage = PageManager.deserializePage("data/" + strTableName + "/" + primaryKey + "/indices/BRIN/"
						+ "page_" + indexPageNumber + ".ser");

				// search the page, find the position of the entry
				int positionInIndex = searchIndex(indexPage.getRows(), (Comparable) htblColNameValue.get(primaryKey),
						primaryKey);

				// if not within any range in this index page, load the next
				// page
				if (positionInIndex == -1) {
					indexPageNumber++;
					continue;
				}

				// if target is within a range in this index page
				else {
					// get pageNumber
					int pageNumber;
					pageNumber = (int) indexPage.getRows()[positionInIndex].get("pageNumber");

					// load page
					Page pageToInsertAt = null;

					try {
						pageToInsertAt = PageManager
								.deserializePage("data/" + strTableName + "/" + "page_" + pageNumber + ".ser");
					} catch (IOException | ClassNotFoundException e) {
						// unexpected error
						// could not load the page
						e.printStackTrace();
					}

					// search for a position to insert at
					int positionToInsertAt = getRowPositionToInsertAt(pageToInsertAt, primaryKey, htblColNameValue);

					// check the position

					// cannot insert in this page
					if (positionToInsertAt == -1) {
						pageNumber++;
						positionToInsertAt = 0;
					}

					return new int[] { pageNumber, positionToInsertAt };

				}

			} catch (IOException | ClassNotFoundException e) {
				// no more index pages
				// get pageNumber; it must be the last entry in the previous
				// index page
				// because of how searchIndexForInsertion() works

				// column was indexed but the current table has no values
				if (indexPage == null) {
					return new int[] { 1, 0 };
				}

				int pageNumber;
				pageNumber = (int) indexPage.getRows()[indexPage.getRows().length - 1].get("pageNumber");

				// load page
				Page pageToInsertAt = null;

				try {
					pageToInsertAt = PageManager
							.deserializePage("data/" + strTableName + "/" + "page_" + pageNumber + ".ser");
				} catch (IOException | ClassNotFoundException e2) {
					// unexpected error
					// could not load the page
					e2.printStackTrace();
				}

				// search for a position to insert at
				int positionToInsertAt = getRowPositionToInsertAt(pageToInsertAt, primaryKey, htblColNameValue);

				// check the position

				// cannot insert in this page, insert in next
				if (positionToInsertAt == -1) {
					pageNumber++;
					positionToInsertAt = 0;
				}

				return new int[] { pageNumber, positionToInsertAt };
			}
		}
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	private static int searchIndex(Hashtable<String, Object>[] rows, Comparable target, String primaryKey) {
		for (int i = 0; i < rows.length; i++) {

			// the index's page is not fully empty and we reached the end of its
			// entries
			// I assume that there are no fully empty index pages
			if (rows[i] == null) {
				return i - 1;
			}

			Comparable currentRowMax = (Comparable) rows[i].get(primaryKey + "Max");
			Comparable currentRowMin = (Comparable) rows[i].get(primaryKey + "Min");

			// if the target belongs in the range
			if (target.compareTo(currentRowMax) <= 0 && target.compareTo(currentRowMin) >= 0) {
				return i;
			}

			// if the target passed is between two ranges
			if (target.compareTo(currentRowMin) < 0) {
				return i;
			}
		}
		// no place found
		return -1;
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected static int getRowPositionToInsertAt(Page currentPage, String primaryKey,
			Hashtable<String, Object> tupleToInsert) throws DBAppException {
		int rowNumber = 0;
		Hashtable<String, Object>[] table = currentPage.getRows();
		for (int i = 0; i < currentPage.getMaxRows(); i++) {
			// The table row is empty
			if (table[i] == null) {
				rowNumber = i;
				return rowNumber;
			}

			// The table row is not empty
			Hashtable<String, Object> currentRow = table[i];

			// compare the primary keys
			int comparisonResult = ((Comparable) tupleToInsert.get(primaryKey))
					.compareTo((Comparable) currentRow.get(primaryKey));

			// The primary key values from toInsertTuple and currentRow primary
			// key
			// are exactly alike
			if (comparisonResult == 0) {
				// check if the matching tuple was deleted
				if (((boolean) currentRow.get("isDeleted"))) {
					rowNumber = i;
					return rowNumber;
				} else {
					throw new DBAppException("The primary key constraint was violated.");
				}
			}

			// The primary key value from toInsertTuple was less than the value
			// from
			// currentRow
			if (comparisonResult < 0) {
				rowNumber = i;
				return rowNumber;
			}
		}

		// The whole page contained values less than the one we want to insert
		// Therefore, we insert in the first position of the next page
		return -1;
	}

	// returns an array list of the pages in the dense index that were changed
	protected static ArrayList<Integer> updateDenseIndexAfterInsertion(String tableName, String columnName,
			int numberOfPageOfInsertion, int rowNumberOfInsertion, Object value) throws DBAppException {

		int relationPageNumber = numberOfPageOfInsertion;
		int relationRowNumber = rowNumberOfInsertion;

		// get the deletion state of the newly inserted element
		Page page = PageManager.loadPageIfExists("data/" + tableName + "/" + "page_" + relationPageNumber + ".ser");
		boolean isDeleted = (boolean) page.getRows()[relationRowNumber].get("isDeleted");

		// point to the value after the insertion
		relationRowNumber++;

		whileLoop: while (true) {
			try {
				// load relation page
				Page currentPage = PageManager
						.deserializePage("data/" + tableName + "/" + "page_" + relationPageNumber + ".ser");

				// loop over the values
				Hashtable<String, Object>[] relationRows = currentPage.getRows();

				for (int i = relationRowNumber; i < relationRows.length; i++) {

					if (relationRows[i] == null) {
						// add new value
						ArrayList<Integer> addNewValueChanges = IndexUtilities.addNewValueToDenseIndex(
								numberOfPageOfInsertion, rowNumberOfInsertion, columnName, tableName, value, isDeleted);

						return addNewValueChanges;
					}

					int previousRowNumber;
					int previousPageNumber;

					// calculate where the entry was previously
					// not first row
					if (i - 1 >= 0) {
						previousRowNumber = i - 1;
						previousPageNumber = currentPage.getPageNumber();
					}

					// at first row
					else {
						// we are at the first page
						if (currentPage.getPageNumber() == 1) {
							previousPageNumber = 1;
							previousRowNumber = 0;
						}

						// we are not at the first page
						else {
							previousPageNumber = currentPage.getPageNumber() - 1;
							previousRowNumber = currentPage.getRows().length - 1;
						}

					}

					// previous entry
					Hashtable<String, Object> previousIndexEntry = new Hashtable<>();
					previousIndexEntry.put("pageNumber", previousPageNumber);
					previousIndexEntry.put("locInPage", previousRowNumber);
					previousIndexEntry.put("value", relationRows[i].get(columnName));
					previousIndexEntry.put("isDeleted", relationRows[i].get("isDeleted"));

					// new entry
					Hashtable<String, Object> newIndexEntry = new Hashtable<>();
					newIndexEntry.put("pageNumber", currentPage.getPageNumber());
					newIndexEntry.put("locInPage", i);
					newIndexEntry.put("value", relationRows[i].get(columnName));
					newIndexEntry.put("isDeleted", relationRows[i].get("isDeleted"));

					// for debugging purposes
					// System.out.println(previousIndexEntry);
					// System.out.println(newIndexEntry);

					if (IndexUtilities.checkForRelationConsecutiveDuplicates(currentPage, i, tableName, columnName)) {
						int[] resumeUpdateDenseIndexAt = new int[2];
						try {
							resumeUpdateDenseIndexAt = IndexUtilities
									.updateConsecutiveDuplicatesInDenseIndex(previousIndexEntry, tableName, columnName);
						} catch (IOException e) {
							e.printStackTrace();
						}

						relationPageNumber = resumeUpdateDenseIndexAt[0];
						relationRowNumber = resumeUpdateDenseIndexAt[1];
						continue whileLoop;

					} else {
						IndexUtilities.findAndReplaceInDenseIndex(tableName, columnName, previousIndexEntry,
								newIndexEntry);
					}

				}

				// proceed to the next page
				relationPageNumber++;
				relationRowNumber = 0;

			} catch (IOException | ClassNotFoundException e) {
				// no more pages

				// add new value
				ArrayList<Integer> addNewValueChanges = IndexUtilities.addNewValueToDenseIndex(numberOfPageOfInsertion,
						rowNumberOfInsertion, columnName, tableName, value, isDeleted);

				return addNewValueChanges;
			}
		}
	}
}