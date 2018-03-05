package team10;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
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
			Hashtable<String, Object> htblColNameValue) throws IOException {

		Page page = InsertionUtilities.loadPage(tableName, positionToInsertAt[0]);
		int maxRows = PageManager.getMaximumRowsCountinPage();
		htblColNameValue.put("TouchDate", new Date());
		htblColNameValue.put("isDeleted", false);
		Hashtable<String, Object> tempHtblColNameValue;

		for (int i = positionToInsertAt[1]; i < maxRows; i++) {

			tempHtblColNameValue = page.getRows()[i];
			page.getRows()[i] = htblColNameValue;
			htblColNameValue = tempHtblColNameValue;

			if (htblColNameValue == null || ((boolean) htblColNameValue.get("isDeleted")) == true)
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
				indexPage = PageManager.deserializePage("data/" + strTableName + "/" + primaryKey + "/indices/BRIN"
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
			if (comparisonResult == -1) {
				rowNumber = i;
				return rowNumber;
			}
		}

		// The whole page contained values less than the one we want to insert
		// Therefore, we insert in the first position of the next page
		return -1;
	}

	protected static ArrayList<Integer> updateDenseIndexAfterInsertion(String tableName, String columnName,
			int numberOfPageOfInsertion, int rowNumberOfInsertion, Object value) {

		int relationPageNumber = numberOfPageOfInsertion;
		int relationRowNumber = rowNumberOfInsertion;

		// array list to use in updating the brin index
		ArrayList<Integer> changedPagesInDenseIndex = new ArrayList<>();

		addNewValueToDenseIndex(numberOfPageOfInsertion, rowNumberOfInsertion, columnName, value);

		// point to the value after the insertion
		// comparison works only because the dense index has the same size as
		// the original relation
		try {
			if (relationRowNumber + 1 > PageManager.getMaximumRowsCountinPage() - 1) // relationRowNumber+1>199
				relationRowNumber = 0;
			else
				relationRowNumber++;
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		while (true) {
			try {
				// load relation page
				Page currentPage = PageManager.deserializePage("data/" + tableName + "/" + columnName + "/indices/BRIN"
						+ "page_" + relationPageNumber + ".ser");

				// loop over the values
				Hashtable<String, Object>[] relationRows = currentPage.getRows();

				for (int i = relationRowNumber; i < relationRows.length; i++) {

					if (relationRows[i] == null) {
						return changedPagesInDenseIndex;
					}
					int previousRowNumber = relationRowNumber - 1;
					int previousPageNumber = currentPage.getPageNumber();
					if (previousRowNumber < 0) {
						previousRowNumber = currentPage.getMaxRows() - 1;
						previousPageNumber = currentPage.getPageNumber() - 1;
					}

					Hashtable<String, Object> previousIndexEntry = new Hashtable<>();
					previousIndexEntry.put("pageNumber", previousPageNumber);
					previousIndexEntry.put("locInPage", previousRowNumber);
					previousIndexEntry.put("value", relationRows[relationRowNumber].get(columnName));

					Hashtable<String, Object> newIndexEntry = new Hashtable<>();
					previousIndexEntry.put("pageNumber", currentPage.getPageNumber());
					previousIndexEntry.put("locInPage", relationRowNumber);
					previousIndexEntry.put("value", relationRows[relationRowNumber].get(columnName));

					int editedIndexPage = findAndReplaceInDenseIndex(tableName, columnName, previousIndexEntry,
							newIndexEntry);

					if (!changedPagesInDenseIndex.contains(editedIndexPage)) {
						changedPagesInDenseIndex.add(editedIndexPage);
					}

				}
				// proceed to the next page
				relationPageNumber++;
				relationRowNumber = 0;

			} catch (IOException | ClassNotFoundException e) {
				// no more pages
				return changedPagesInDenseIndex;
			}
		}
	}

	protected static void addNewValueToDenseIndex(int relationPageNumber, int relationRowNumber, String columnName,
			Object newValue) {
		// stick to the strict sorting here

	}

	protected static int findAndReplaceInDenseIndex(String tableName, String columnName,
			Hashtable<String, Object> oldEntry, Hashtable<String, Object> newEntry) {
		// binary search can return -1 or -2, check below

		return -1;
	}

	protected static int denseIndexBinarySearch(Hashtable<String, Object>[] indexPageRows, int left, int right,
			Hashtable<String, Object> target) {
		if (right >= left) {
			int mid = left + (right - left) / 2;

			// if a null value is encountered, then the page is not full
			// signal that a linear search should start with -2
			if (indexPageRows[mid] == null)
				return -2;

			// If the element is present at the
			// middle itself
			if (compareIndexElements(indexPageRows[mid], target) == 0)
				return mid;

			// If element is smaller than mid, then
			// it can only be present in left subarray
			if (compareIndexElements(indexPageRows[mid], target) == -1)
				return denseIndexBinarySearch(indexPageRows, left, mid - 1, target);

			// Else the element can only be present
			// in right subarray
			return denseIndexBinarySearch(indexPageRows, mid + 1, right, target);
		}

		// We reach here when element is not present
		// in array
		return -1;
	}
	
	protected static int compareIndexElements(Hashtable<String,Object> pageEntry, Hashtable<String,Object> otherEntry){
		//implement an incremental key comparison
			
		}
		
	}

}