package team10;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Hashtable;

public class IndexUtilities {

	// Checks if the directory of table exists
	protected static boolean tableDirectoryExists(String strTableName) throws DBAppException {
		File tableFile = new File("data/" + strTableName);
		if (tableFile.exists() && tableFile.isDirectory()) {
			return true;
		}
		return false;
	}

	// Checks in the meta data whether the column represents a primary key or
	// not
	protected static boolean isColumnPrimary(String columnMeta) throws DBAppException {
		if (columnMeta == null) {
			throw new DBAppException("meta data retrieval error");
		}
		String[] columnParams = columnMeta.split(",");
		if (columnParams[3].equals("true")) {
			return true;
		}
		return false;

	}

	protected static boolean isColumnIndexed(String columnMeta) throws DBAppException {
		if (columnMeta == null) {
			throw new DBAppException("meta data retrieval error");
		}
		String[] columnParams = columnMeta.split(",");
		if (columnParams[4].equals("true")) {
			return true;
		}
		return false;

	}

	// Retrieves meta data of specific column in specific table
	protected static String retrieveColumnMetaInTable(String strTableName, String strColumnName) {
		BufferedReader metaReader;
		try {
			metaReader = new BufferedReader(new FileReader("data/metadata.csv"));
			String metaLine = metaReader.readLine();
			while (metaLine != null) {
				String[] tableParams = metaLine.split(",");
				if (tableParams[0].equals(strTableName) && tableParams[1].equals(strColumnName)) {
					metaReader.close();
					return metaLine;
				}
				metaLine = metaReader.readLine();
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;

	}

	// BRIN index business logic for now
	protected static void createBRINFiles(String strTableName, String strColumnName, boolean isPrimary)
			throws Exception {
		if (!isPrimary) {
			makeIndexDirectory(strTableName, strColumnName, "Dense");
			createDenseIndex(strTableName, strColumnName);
		}
		makeIndexDirectory(strTableName, strColumnName, "BRIN");
		if (isPrimary) {
			updateBRINIndexOnPK(strTableName, strColumnName, 1);
		} else {
			updateBRINIndexOnDense(strTableName, strColumnName,
					retrieveAllPageNumbers("data/" + strTableName + "/" + strColumnName + "/indices/Dense"));
		}
	}

	// Creates dense index of the given column in the given table
	protected static void createDenseIndex(String strTableName, String strColumnName) throws Exception {
		int tablePageNumber = 1;
		String tempDirName = "Temp";

		Path tmpDirPath = moveTuplesToDir(strTableName);
		new File("data/" + strTableName + "/" + strColumnName + "/indices/Dense/").mkdirs();
		setColumnIndexed(strTableName, strColumnName);

		while (true) {

			Page tablePage;

			try {

				tablePage = PageManager.deserializePage(tmpDirPath.toString() + "page_" + tablePageNumber++ + ".ser");

			} catch (ClassNotFoundException | IOException e) {

				System.out.println("Dense Creation Is Done!");
				new File("data/" + strTableName + "/" + tempDirName + "/").delete();
				return;

			}

			for (Hashtable<String, Object> tuple : tablePage.getRows())
				if (tuple != null)
					altInsertion(strTableName, tuple, strColumnName);

		}

	}

	protected static Path moveTuplesToDir(String strTableName) throws IOException {

		// TODO: Check if table exists

		File tableDir = new File("data/" + strTableName + "/");
		Path tmpDirPath = Files.createTempDirectory("DBAppTeam10-");

		for (File file : tableDir.listFiles())
			if (!file.isDirectory() && file.getName().endsWith(".ser")) {
				file.renameTo(new File(tmpDirPath.toString() + file.getName()));
				file.delete();
			}

		return tmpDirPath;

	}

	protected static void setColumnIndexed(String strTableName, String strColName) throws IOException {

		String csvFullText = "";
		String line = null;
		BufferedReader br = new BufferedReader(new FileReader("data/metadata.csv"));

		while ((line = br.readLine()) != null) {
			String[] content = line.split(",");

			if (content[0].equals(strTableName) && content[1].equals(strColName))
				csvFullText += content[0] + "," + content[1] + "," + content[2] + "," + content[3] + "," + "true"
						+ "\n";
			else
				csvFullText += line + "\n";

			PrintWriter writer = new PrintWriter("data/metadata.csv");
			writer.print(csvFullText);
			writer.close();

		}

		br.close();

	}

	protected static void updateBRINIndexOnPK(String tableName, String columnName, int changedPageNumber)
			throws ClassNotFoundException, IOException, DBAppException {
		File currentTablePageFile = new File("data/" + tableName + "/page_" + changedPageNumber + ".ser");
		while (currentTablePageFile.exists()) {
			int currentBRINPageLoc = ((changedPageNumber - 1) / PageManager.getBRINSize()) + 1;
			Page currentTablePage = PageManager.deserializePage(currentTablePageFile.getPath());
			Object[] minAndMaxInCurrentTablePage = retrieveMinAndMaxInPage(currentTablePage, false, columnName);
			Page BRINIndexPage = null;
			try {
				// in case the BRIN index page exists retrieve it
				BRINIndexPage = retrievePage("data/" + tableName + "/" + columnName + "/indices/BRIN",
						currentBRINPageLoc);
			} catch (DBAppException e) {
				// if it does not exists create a new page
				if (e.getMessage().equals("Error!The page file does not exist")) {
					BRINIndexPage = new Page(currentBRINPageLoc, PageType.BRIN);
				} else {
					throw new DBAppException("cannot process BRIN index page");
				}
			}
			Hashtable<String, Object>[] BRINRecords = BRINIndexPage.getRows();
			int locOfTablePageRecordInBRIN = retrieveLocOfPageRecordInBRIN(changedPageNumber - 1);
			if (BRINRecords[locOfTablePageRecordInBRIN] == null) {
				BRINRecords[locOfTablePageRecordInBRIN] = new Hashtable<String, Object>();
			}
			updateBRINRecord(columnName, BRINRecords[locOfTablePageRecordInBRIN], minAndMaxInCurrentTablePage,
					changedPageNumber);
			updateDeletedFlagOnBRINRecord(BRINRecords[locOfTablePageRecordInBRIN], currentTablePage);
			PageManager.serializePage(BRINIndexPage,
					"data/" + tableName + "/" + columnName + "/indices/BRIN/page_" + currentBRINPageLoc + ".ser");
			changedPageNumber++;
			currentTablePageFile = new File("data/" + tableName + "/page_" + changedPageNumber + ".ser");
		}
	}

	protected static void updateBRINIndexOnDense(String tableName, String columnName,
			ArrayList<Integer> changedDenseIndexPages) throws DBAppException, IOException {
		for (int i = 0; i < changedDenseIndexPages.size(); i++) {

			// get the target dense index page to update the BRIN

			int currentDensePageLoc = (int) (changedDenseIndexPages.get(i)) - 1;

			// calculate the number of the BRIN page contaning the dense index
			// page info
			int currentBRINPageLoc = (currentDensePageLoc / PageManager.getBRINSize()) + 1;
			// retrieve the dense index page to be updated

			Page currentDenseIndexPage = retrievePage("data/" + tableName + "/" + columnName + "/indices/Dense",
					currentDensePageLoc + 1);
			Object[] minAndMaxInCurrentPage = retrieveMinAndMaxInPage(currentDenseIndexPage, true, columnName);
			Page BRINIndexPage = null;
			try {
				// in case the BRIN index page exists retrieve it
				BRINIndexPage = retrievePage("data/" + tableName + "/" + columnName + "/indices/BRIN",
						currentBRINPageLoc);
			} catch (DBAppException e) {
				// if it does not exists create a new page
				if (e.getMessage().equals("Error!The page file does not exist")) {
					BRINIndexPage = new Page(currentBRINPageLoc, PageType.BRIN);
				} else {
					e.printStackTrace();
				}
			}
			Hashtable<String, Object>[] BRINRecords = BRINIndexPage.getRows();
			int locOfDenseRecordInBRIN = retrieveLocOfPageRecordInBRIN(currentDensePageLoc);
			if (BRINRecords[locOfDenseRecordInBRIN] == null) {
				BRINRecords[locOfDenseRecordInBRIN] = new Hashtable<String, Object>();
			}
			updateBRINRecord(columnName, BRINRecords[locOfDenseRecordInBRIN], minAndMaxInCurrentPage,
					currentDensePageLoc + 1);
			updateDeletedFlagOnBRINRecord(BRINRecords[locOfDenseRecordInBRIN], currentDenseIndexPage);
			PageManager.serializePage(BRINIndexPage,
					"data/" + tableName + "/" + columnName + "/indices/BRIN/page_" + currentBRINPageLoc + ".ser");
		}
	}

	// @SuppressWarnings("unchecked")
	// protected static int addNewBRINRecord(String columnName, Page
	// BRINIndexPage,
	// Object[] newMinAndMaxValues) {
	// Hashtable<String, Object>[] BRINRecords = BRINIndexPage.getRows();
	// ArrayList<Hashtable<String, Object>> BRINRecordList = new
	// ArrayList<Hashtable<String, Object>>(
	// Arrays.asList(BRINRecords));
	// Hashtable<String, Object> newRecord = new Hashtable<String, Object>();
	// updateBRINRecord(columnName, newRecord, newMinAndMaxValues);
	// BRINRecordList.add(newRecord);
	// Collections.sort(BRINRecordList, getBRINIndexComparator());
	// int locOfNewRecord = BRINRecordList.indexOf(newRecord);
	// BRINRecords = (Hashtable<String, Object>[]) BRINRecordList.toArray();
	// BRINIndexPage.setRows(BRINRecords);
	// return locOfNewRecord;
	// }

	protected static void updateBRINRecord(String columnName, Hashtable<String, Object> BRINRecord,
			Object[] newMinAndMaxValues, int densePageLoc) {
		BRINRecord.put(columnName + "Min", newMinAndMaxValues[0]);
		BRINRecord.put(columnName + "Max", newMinAndMaxValues[1]);
		BRINRecord.put("pageNumber", densePageLoc);
	}

	protected static void updateDeletedFlagOnBRINRecord(Hashtable<String, Object> BRINRecord, Page denseIndexPage) {
		Hashtable<String, Object>[] denseIndexPageRecords = denseIndexPage.getRows();
		for (int i = 0; i < denseIndexPageRecords.length; i++) {
			if (!((boolean) (denseIndexPageRecords[i].get("isDeleted")))) {
				BRINRecord.put("isDeleted", false);
				return;
			}
		}
		BRINRecord.put("isDeleted", true);

	}

	protected static Page retrievePage(String pageDirectoryPath, int pageNumber) throws DBAppException {
		// Get a page based on the containing directory path
		// Throws a DBAppException in case the file path does not point to a
		// directory
		// Throws a DBAppException in case the page does not exist
		File pageDirectory = new File(pageDirectoryPath);
		if (!pageDirectory.exists()) {
			throw new DBAppException("The file path supplied does not exist");
		}
		if (!pageDirectory.isDirectory()) {
			throw new DBAppException("The file path supplied to retrieve the page is not a directory");
		}
		File pageFile = new File(pageDirectoryPath + "/page_" + pageNumber + ".ser");
		if (!pageFile.exists()) {
			throw new DBAppException("The page file does not exist");
		}
		try {
			return PageManager.deserializePage(pageFile.getPath());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;

	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	protected static Object[] retrieveMinAndMaxInPage(Page targetPage, boolean isIndex, String columnName) {
		Hashtable<String, Object>[] pageRows = targetPage.getRows();
		Comparable minValueInPage = null;
		Comparable maxValueInPage = null;
		if (isIndex) {
			minValueInPage = (Comparable) pageRows[0].get("value");
			maxValueInPage = (Comparable) pageRows[0].get("value");

		} else {
			minValueInPage = (Comparable) pageRows[0].get(columnName);
			maxValueInPage = (Comparable) pageRows[0].get(columnName);
		}
		try {
			for (int i = 0; i < pageRows.length; i++) {
				Hashtable<String, Object> currentRecord = pageRows[i];
				Comparable currentValue = null;
				if (isIndex) {
					currentValue = (Comparable) currentRecord.get("value");
				} else {
					currentValue = (Comparable) currentRecord.get(columnName);
				}
				if (currentValue == null)
					break;

				if (currentValue.compareTo(minValueInPage) < 0) {
					minValueInPage = currentValue;
				} else if (currentValue.compareTo(maxValueInPage) > 0) {
					maxValueInPage = currentValue;
				}
			}
		} catch (NullPointerException e) {
			/*
			 * in case the you read a null value, that means that the page has
			 * empty records and no more values are in it.
			 */
			Object[] minAndMaxValues = { (Object) minValueInPage, (Object) maxValueInPage };
			return minAndMaxValues;
		}
		Object[] minAndMaxValues = { (Object) minValueInPage, (Object) maxValueInPage };
		return minAndMaxValues;

	}

	protected static Comparator<Hashtable<String, Object>> getBRINIndexComparator() {
		return new Comparator<Hashtable<String, Object>>() {
			public int compare(Hashtable<String, Object> record1, Hashtable<String, Object> record2) {
				int record1PageNumber = (int) (record1.get("pageNumber"));
				int record2PageNumber = (int) (record2.get("pageNumber"));
				return record1PageNumber < record2PageNumber ? -1 : record1PageNumber > record2PageNumber ? 1 : 0;
			}
		};
	}

	protected static int retrieveLocOfPageRecordInBRIN(int DensePageNumber) throws IOException {
		return DensePageNumber % PageManager.getBRINSize();
	}

	protected static void EraseNonExistentDenseIndexPages(String tableName, String columnName,
			ArrayList<Integer> changedDenseIndexPages) {
		/*
		 * the check is only on the maximum page numbers in case a max page
		 * number does not exist, remove it from the arraylist then recheck
		 * exists the max page exists again for robustness once the check for
		 * existence of page passes, break the loop
		 */
		do {
			int maxPageNumber = Collections.max(changedDenseIndexPages);
			File maxPage = new File(
					"data/" + tableName + "/" + columnName + "/indices/Dense/page_" + maxPageNumber + ".ser");
			if (maxPage.exists()) {
				break;
			} else {
				changedDenseIndexPages.remove(maxPageNumber);
			}

		} while (changedDenseIndexPages.isEmpty());

	}

	// Get a page based on the containing directory path
	// Throws a DBAppException in case the file path does not point to a
	// directory
	// Throws a DBAppException in case the page does not exist

	// Retrieve all pages in a given path
	protected static ArrayList<Integer> retrieveAllPageNumbers(String filepath)
			throws IOException, ClassNotFoundException {
		ArrayList<Integer> pageNumbers = new ArrayList<Integer>();
		IndexUtilities.validateDirectory(filepath);
		ArrayList<Page> pages = new ArrayList<Page>();
		File dir = new File(filepath);

		for (File file : dir.listFiles()) {

			String name = file.getName();
			if (name.substring(0, 5).equals("page_") && name.substring(name.indexOf('.')).equals(".ser"))
				pageNumbers.add(Integer.parseInt(name.charAt(5) + ""));

		}

		return pageNumbers;

	}

	// check a file path and create directories that don't exist through the
	// file
	// path on the file system
	protected static void validateDirectory(String filepath) throws IOException {

		String[] pathParams = filepath.split("/");
		filepath = "";
		int i = 0;

		do {

			filepath += (i == 0) ? pathParams[i++] : "/" + pathParams[i++];

			File file = new File(filepath);

			if (!file.exists())
				file.mkdir();

		} while (i < pathParams.length);

	}

	/*
	 * make directories for the appropriate index type
	 */
	protected static void makeIndexDirectory(String tableName, String columnName, String indexType)
			throws DBAppException {
		if (indexType.equals("Dense")) {
			File indexDir = new File("data/" + tableName + "/" + columnName + "/indices/" + "Dense");
			if (!indexDir.exists()) {
				indexDir.mkdirs();
			}
		} else if (indexType.equals("BRIN")) {
			File indexDir = new File("data/" + tableName + "/" + columnName + "/indices/" + "BRIN");
			if (!indexDir.exists()) {
				indexDir.mkdirs();
			}
		} else {
			throw new DBAppException("The only supported index types are dense and BRIN indices");
		}
	}

	// revise if errors occur
	protected static ArrayList<Integer> addNewValueToDenseIndex(int relationPageNumber, int relationRowNumber,
			String columnName, String tableName, Object newValue, boolean isDeletedValue) throws DBAppException {

		Hashtable<String, Object> newEntry = new Hashtable<>();
		newEntry.put("value", newValue);
		newEntry.put("pageNumber", relationPageNumber);
		newEntry.put("locInPage", relationRowNumber);
		newEntry.put("isDeleted", isDeletedValue);

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
			int lastChangedPage = insertIntoDenseIndex(tableName, columnName, pageNumber, targetLocation, newEntry);
			ArrayList<Integer> changedPages = new ArrayList<Integer>();

			for (int j = pageNumber; j <= lastChangedPage; j++)
				changedPages.add(j);

			return changedPages;

		} catch (IOException e) {
			// some error occurred
			e.printStackTrace();
			throw new DBAppException();
		}

	}

	protected static int insertIntoDenseIndex(String tableName, String columnName, int pageNumber, int rowNumber,
			Hashtable<String, Object> htblColNameValue) throws IOException {
		Page page = loadDenseIndexPage(tableName, columnName, pageNumber);
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
				page = loadDenseIndexPage(tableName, columnName, ++pageNumber);

			}

		}

		PageManager.serializePage(page,
				"data/" + tableName + "/" + columnName + "/indices/Dense/" + "page_" + page.getPageNumber() + ".ser");
		return page.getPageNumber();
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

			if (nextPage.getRows()[0] == null)
				// get the next value from the first row
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
		int currentRow = denseIndexLinearSearch(currentIndexPage, previousEntry);

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

	protected static void altInsertion(String strTableName, Hashtable<String, Object> htblColNameValue,
			String newlyIndexedColumn) throws Exception {
		String line = null;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader("data/metadata.csv"));
			line = br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}

		ArrayList<String[]> data = new ArrayList<>();
		String primaryKey = null;
		ArrayList<String> indexedColumns = new ArrayList<>();
		Hashtable<String, String> ColNameType = new Hashtable<>();

		while (line != null) {
			String[] content = line.split(",");

			if (content[0].equals(strTableName)) {
				data.add(content);
				ColNameType.put(content[1], content[2]);
				if ((content[3].toLowerCase()).equals("true"))
					primaryKey = content[1];
				if (content[4].toLowerCase().equals("true"))
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

		if (data.isEmpty())
			throw new DBAppException("404 Table Not Found !");

		if (htblColNameValue.get(primaryKey).equals(null))
			throw new DBAppException("Primary Key Can NOT be null");

		if (!InsertionUtilities.isValidTuple(ColNameType, htblColNameValue))
			throw new DBAppException(
					"The tuple you're trying to insert into table " + strTableName + " is not a valid tuple!");

		int[] positionToInsertAt;
		if (indexedColumns.contains(primaryKey)) {
			positionToInsertAt = InsertionUtilities.searchForInsertionPositionIndexed(strTableName, primaryKey,
					htblColNameValue);
		} else {
			positionToInsertAt = InsertionUtilities.searchForInsertionPosition(strTableName, primaryKey,
					htblColNameValue);
		}
		// for some reason, Maq's insertTuple modifies the positionToInsertAt.
		// Therefore, this local array is needed
		int[] tempPositionToInsertAt = { positionToInsertAt[0], positionToInsertAt[1] };

		try {
			InsertionUtilities.insertTuple(strTableName, positionToInsertAt, htblColNameValue, false);
		} catch (IOException e) {
			e.printStackTrace();
		}

		ArrayList<Integer> changedPagesAfterDenseIndexUpdate = new ArrayList<Integer>();

		if (!newlyIndexedColumn.equals(primaryKey)) {
			changedPagesAfterDenseIndexUpdate = InsertionUtilities.updateDenseIndexAfterInsertion(strTableName,
					newlyIndexedColumn, tempPositionToInsertAt[0], tempPositionToInsertAt[1],
					htblColNameValue.get(newlyIndexedColumn));
			try {
				IndexUtilities.updateBRINIndexOnDense(strTableName, newlyIndexedColumn,
						changedPagesAfterDenseIndexUpdate);
			} catch (Exception e) {
				e.printStackTrace();
			}
		} else {
			try {
				IndexUtilities.updateBRINIndexOnPK(strTableName, primaryKey, positionToInsertAt[0]);
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		/** TODO update the BRIN index after insertion **/

		System.out.println("Tuple Inserted!");
		System.out.println(
				"Changed Dense Index Page Numbers at the end: " + changedPagesAfterDenseIndexUpdate.toString());

	}

}
