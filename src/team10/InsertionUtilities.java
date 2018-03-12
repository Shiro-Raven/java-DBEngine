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

	protected static ArrayList<Integer> updateDenseIndexAfterInsertion(String tableName, String columnName,
			int numberOfPageOfInsertion, int rowNumberOfInsertion, Object value) {

		int relationPageNumber = numberOfPageOfInsertion;
		int relationRowNumber = rowNumberOfInsertion;

		// array list to use in updating the brin index
		ArrayList<Integer> changedPagesInDenseIndex = new ArrayList<>();

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
						addNewValueToDenseIndex(numberOfPageOfInsertion, rowNumberOfInsertion, columnName, tableName,
								value);
						return changedPagesInDenseIndex;
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

					// new entry
					Hashtable<String, Object> newIndexEntry = new Hashtable<>();
					newIndexEntry.put("pageNumber", currentPage.getPageNumber());
					newIndexEntry.put("locInPage", i);
					newIndexEntry.put("value", relationRows[i].get(columnName));
					System.out.println(previousIndexEntry);
					System.out.println(newIndexEntry);

					if (checkForRelationConsecutiveDuplicates(currentPage, i, tableName, columnName)) {
						int[] resumeUpdateDenseIndexAt = new int[2];
						try {
							resumeUpdateDenseIndexAt = updateConsecutiveDuplicatesInDenseIndex(previousIndexEntry,
									tableName, columnName);
						} catch (IOException e) {
							e.printStackTrace();
						}
						relationPageNumber = resumeUpdateDenseIndexAt[0];
						relationRowNumber = resumeUpdateDenseIndexAt[1];
						continue whileLoop;
					} else {
						int editedIndexPage = findAndReplaceInDenseIndex(tableName, columnName, previousIndexEntry,
								newIndexEntry);

						if (!changedPagesInDenseIndex.contains(editedIndexPage)) {
							changedPagesInDenseIndex.add(editedIndexPage);
						}
					}

				}

				// proceed to the next page
				relationPageNumber++;
				relationRowNumber = 0;

			} catch (IOException | ClassNotFoundException e) {
				// no more pages

				// safety check
				while (changedPagesInDenseIndex.contains(-1)) {
					changedPagesInDenseIndex.remove(new Integer(-1));
				}

				addNewValueToDenseIndex(numberOfPageOfInsertion, rowNumberOfInsertion, columnName, tableName, value);
				return changedPagesInDenseIndex;
			}
		}
	}

	// revise if errors occur
	protected static void addNewValueToDenseIndex(int relationPageNumber, int relationRowNumber, String columnName,
			String tableName, Object newValue) {

		Hashtable<String, Object> newEntry = new Hashtable<>();
		newEntry.put("value", newValue);
		newEntry.put("pageNumber", relationPageNumber);
		newEntry.put("locInPage", relationRowNumber);

		int pageNumber = 1;
		int targetLocation = 0;

		Loop: while (true) {
			Page currentPage = null;
			try {
				currentPage = PageManager.deserializePage(
						"data/" + tableName + "/" + columnName + "/indices/Dense/page_" + pageNumber + ".ser");
			} catch (IOException | ClassNotFoundException e) {
				// we need a new page
				targetLocation = 0;
				break Loop;
			}
			Hashtable<String, Object>[] rows = currentPage.getRows();
			for (int i = 0; i < rows.length; i++) {
				if (rows[i] == null) {
					targetLocation = i;
					break Loop;
				}

				// if page entry is bigger in value
				if (compareIndexElements(rows[i], newEntry) == -1) {
					targetLocation = i;
					break Loop;
				}
			}
			pageNumber++;
		}

		try {
			insertIntoDenseIndex(tableName, columnName, pageNumber, targetLocation, newEntry);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	protected static boolean insertIntoDenseIndex(String tableName, String columnName, int pageNumber, int rowNumber,
			Hashtable<String, Object> htblColNameValue) throws IOException {
		Page page = InsertionUtilities.loadDenseIndexPage(tableName, columnName, pageNumber);
		int maxRows = PageManager.getMaximumRowsCountinPage();
		Hashtable<String, Object> tempHtblColNameValue;

		for (int i = rowNumber; i < maxRows; i++) {

			tempHtblColNameValue = page.getRows()[i];
			page.getRows()[i] = htblColNameValue;
			htblColNameValue = tempHtblColNameValue;

			if (htblColNameValue == null)
				break;

			if (i == maxRows - 1) {

				i = -1; // Reset i
				PageManager.serializePage(page, "data/" + tableName + "/" + columnName + "/indices/Dense/" + "page_"
						+ page.getPageNumber() + ".ser");
				page = InsertionUtilities.loadDenseIndexPage(tableName, columnName, ++pageNumber);

			}

		}

		PageManager.serializePage(page,
				"data/" + tableName + "/" + columnName + "/indices/Dense/" + "page_" + page.getPageNumber() + ".ser");
		return true;
	}

	// warning: should not be used to load pages in a loop; the loop will become
	// infinite as new pages are created
	public static Page loadDenseIndexPage(String tableName, String columnName, int pageNumber) throws IOException {

		Page page;

		try {

			page = PageManager.deserializePage(
					"data/" + tableName + "/" + columnName + "/indices/Dense/" + "page_" + pageNumber + ".ser");

		} catch (Exception e) {

			page = new Page(pageNumber);

		}

		return page;

	}

	protected static int findAndReplaceInDenseIndex(String tableName, String columnName,
			Hashtable<String, Object> oldEntry, Hashtable<String, Object> newEntry) {
		// binary search can return -1 or -2, check below
		Page currentPage = null;
		int pageNumber = 1;
		while (true) {
			try {
				currentPage = PageManager.deserializePage(
						"data/" + tableName + "/" + columnName + "/indices/Dense/" + "page_" + pageNumber + ".ser");
			} catch (IOException | ClassNotFoundException e) {
				e.printStackTrace();
				return -1;
			}

			int binarySearchResult = denseIndexBinarySearch(currentPage.getRows(), 0, currentPage.getRows().length - 1,
					oldEntry);

			// null was encountered
			if (binarySearchResult == -2) {
				int linearSearchResult = denseIndexLinearSearch(currentPage, oldEntry);
				if (linearSearchResult != -1) {
					currentPage.getRows()[linearSearchResult] = newEntry;

					try {
						PageManager.serializePage(currentPage, "data/" + tableName + "/" + columnName
								+ "/indices/Dense/" + "page_" + currentPage.getPageNumber() + ".ser");
					} catch (IOException e) {
						e.printStackTrace();
					}

					return currentPage.getPageNumber();
				} else {
					return -1;
				}
			}

			// search yielded a result
			if (binarySearchResult != -1) {
				currentPage.getRows()[binarySearchResult] = newEntry;

				try {
					PageManager.serializePage(currentPage, "data/" + tableName + "/" + columnName + "/indices/Dense/"
							+ "page_" + currentPage.getPageNumber() + ".ser");
				} catch (IOException e) {
					e.printStackTrace();
				}

				return currentPage.getPageNumber();
			}

			// the search yielded no results
			pageNumber++;

		}
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

	protected static int denseIndexLinearSearch(Page indexPage, Hashtable<String, Object> searchValue) {

		Hashtable<String, Object>[] rows = indexPage.getRows();

		for (int i = 0; i < rows.length; i++) {
			if (rows[i] == null)
				return i;
			if (compareIndexElements(rows[i], searchValue) == 0) {
				return i;
			}
		}

		// not found
		return -1;
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected static int compareIndexElements(Hashtable<String, Object> pageEntry,
			Hashtable<String, Object> otherEntry) {
		// implement an incremental key comparison
		if (((Comparable) pageEntry.get("locInPage")).compareTo((Comparable) otherEntry.get("locInPage")) == 0
				&& ((Comparable) pageEntry.get("pageNumber")).compareTo((Comparable) otherEntry.get("pageNumber")) == 0
				&& ((Comparable) pageEntry.get("value")).compareTo((Comparable) otherEntry.get("value")) == 0) {
			return 0;
		} else {
			String[] hashtableKeys = { "value", "pageNumber", "locInPage" };
			for (int i = 0; i < 3; i++) {
				// other entry is less than the page entry
				if (((Comparable) otherEntry.get(hashtableKeys[i]))
						.compareTo((Comparable) pageEntry.get(hashtableKeys[i])) < 0) {
					return -1;
				} else if (((Comparable) otherEntry.get(hashtableKeys[i]))
						.compareTo((Comparable) pageEntry.get(hashtableKeys[i])) == 0) {
					continue;
				} else
					return 1;
			}
		}
		System.out.println("Inside compareIndexElements. Shouldn't be here");
		return 0;
	}

	// checks if duplicates in the original relation occur right after each
	// other
	// This will produce a conflict in the findAndReplaceMethod
	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected static boolean checkForRelationConsecutiveDuplicates(Page currentPage, int currentRow, String tableName,
			String columnName) {

		Hashtable<String, Object>[] currentPageRows = currentPage.getRows();

		// get the value in the current row
		Comparable currentRowValue = (Comparable) currentPageRows[currentRow].get(columnName);

		// find the next row value
		Comparable nextRowValue;

		// we are in the last row of the page
		if (currentRow == currentPageRows.length - 1) {

			Page nextPage = null;

			// load the next page
			try {
				nextPage = PageManager
						.deserializePage("data/" + tableName + "/page_" + (currentPage.getPageNumber() + 1) + ".ser");
			} catch (ClassNotFoundException | IOException e) {
				return false;
			}

			// get the next value from the first row
			if (nextPage.getRows()[0] == null)
				return false;
			nextRowValue = (Comparable) nextPage.getRows()[0].get(columnName);
		}

		// we are not in the last row of the page
		else {
			// get the next value
			if (currentPageRows[currentRow + 1] == null)
				return false;
			nextRowValue = (Comparable) currentPageRows[currentRow + 1].get(columnName);
		}

		// check if nextRowValue is null
		if (nextRowValue == null)
			return false;
		else {
			return currentRowValue.compareTo(nextRowValue) == 0;
		}
	}

	// returns the page number and row number where the normal update method
	// should continue its work
	protected static int[] updateConsecutiveDuplicatesInDenseIndex(Hashtable<String, Object> previousEntry,
			String tableName, String columnName) throws IOException {
		// the previous entry contains the value of the duplicate and its first
		// old occurrence

		// retrieve duplicated value
		int pageOfFirstOccurrence = findAndReplaceInDenseIndex(tableName, columnName, previousEntry, previousEntry);

		// Initializations:
		// load the page of the first occurrence
		Page currentIndexPage = null;

		try {
			currentIndexPage = PageManager.deserializePage("data/" + tableName + "/" + columnName + "/indices/Dense/"
					+ "page_" + pageOfFirstOccurrence + ".ser");
		} catch (ClassNotFoundException | IOException e) {
			e.printStackTrace();
		}

		// find the exact row
		int currentRow = InsertionUtilities.denseIndexLinearSearch(currentIndexPage, previousEntry);

		Page nextEntryPage = null;
		int nextEntryRow;

		// do the replacements
		while (true) {

			// end of page
			if (currentRow == currentIndexPage.getRows().length || currentIndexPage.getRows()[currentRow] == null) {
				try {
					// serialize the page
					PageManager.serializePage(currentIndexPage, "data/" + tableName + "/" + columnName
							+ "/indices/Dense/" + "page_" + currentIndexPage.getPageNumber() + ".ser");

					// load the next page
					currentIndexPage = PageManager.deserializePage("data/" + tableName + "/" + columnName
							+ "/indices/Dense/" + "page_" + (currentIndexPage.getPageNumber() + 1) + ".ser");
					currentRow = 0;
				} catch (ClassNotFoundException | IOException e) {
					// no more pages
					break;
				}
			}

			// determine next entry location
			if (currentRow == currentIndexPage.getRows().length - 1) {
				try {
					nextEntryPage = PageManager.deserializePage("data/" + tableName + "/" + columnName
							+ "/indices/Dense/" + "page_" + (currentIndexPage.getPageNumber() + 1) + ".ser");
					nextEntryRow = 0;
				} catch (ClassNotFoundException | IOException e) {
					// no more pages
					break;
				}
			} else {
				nextEntryRow = currentRow + 1;
				nextEntryPage = currentIndexPage;
			}

			// check if the next value is a consecutive duplicate
			if (isConsecutiveDuplicate(currentIndexPage.getRows()[currentRow], nextEntryPage.getRows()[nextEntryRow])) {
				if ((int) currentIndexPage.getRows()[currentRow].get("locInPage") == PageManager
						.getMaximumRowsCountinPage() - 1) {
					currentIndexPage.getRows()[currentRow].put("locInPage", 0);
					currentIndexPage.getRows()[currentRow].put("pageNumber",
							(int) currentIndexPage.getRows()[currentRow].get("pageNumber") + 1);
				} else {
					currentIndexPage.getRows()[currentRow].put("locInPage",
							(int) currentIndexPage.getRows()[currentRow].get("locInPage") + 1);
				}
			} else {
				break;
			}

			// increment the row counter
			currentRow++;

		}

		// update the last duplicate value

		if ((int) currentIndexPage.getRows()[currentRow].get("locInPage") == PageManager.getMaximumRowsCountinPage()
				- 1) {
			currentIndexPage.getRows()[currentRow].put("locInPage", 0);
			currentIndexPage.getRows()[currentRow].put("pageNumber",
					(int) currentIndexPage.getRows()[currentRow].get("pageNumber") + 1);
		} else {
			currentIndexPage.getRows()[currentRow].put("locInPage",
					(int) currentIndexPage.getRows()[currentRow].get("locInPage") + 1);
		}

		// serialize the page
		PageManager.serializePage(currentIndexPage, "data/" + tableName + "/" + columnName + "/indices/Dense/" + "page_"
				+ currentIndexPage.getPageNumber() + ".ser");

		// return the location of where to resume from
		return resumeDenseIndexUpdateAt((int) currentIndexPage.getRows()[currentRow].get("pageNumber"),
				(int) currentIndexPage.getRows()[currentRow].get("locInPage"));
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected static boolean isConsecutiveDuplicate(Hashtable<String, Object> currentEntry,
			Hashtable<String, Object> nextEntry) throws IOException {

		if (nextEntry == null || currentEntry == null) {
			return false;
		}

		int currentEntryLocInPage = (int) currentEntry.get("locInPage");
		int currentEntryPageNumber = (int) currentEntry.get("pageNumber");
		Comparable currentEntryValue = (Comparable) currentEntry.get("value");

		int nextEntryLocInPage = (int) nextEntry.get("locInPage");
		int nextEntryPageNumber = (int) nextEntry.get("pageNumber");
		Comparable nextEntryValue = (Comparable) nextEntry.get("value");

		if (currentEntryValue.compareTo(nextEntryValue) != 0)
			return false;
		else {
			if (currentEntryLocInPage == PageManager.getMaximumRowsCountinPage() - 1) {
				return nextEntryPageNumber == currentEntryPageNumber + 1 && nextEntryLocInPage == 0;
			} else {
				return nextEntryPageNumber == currentEntryPageNumber && nextEntryLocInPage == currentEntryLocInPage + 1;
			}
		}
	}

	protected static int[] resumeDenseIndexUpdateAt(int pageNumber, int rowNumber) throws IOException {
		if (rowNumber == PageManager.getMaximumRowsCountinPage() - 1) {
			return new int[] { pageNumber + 1, 0 };
		} else {
			return new int[] { pageNumber, rowNumber + 1 };
		}
	}

}