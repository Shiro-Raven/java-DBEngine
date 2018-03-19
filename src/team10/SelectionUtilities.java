package team10;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

public class SelectionUtilities {

	// the main method for the select functionality
	// according to the column we select on, whether it is the primary key, and
	// whether it is indexed, the path of execution is chosen
	protected static Iterator<Hashtable<String, Object>> selectFromTableHelper(String tableName, String columnName,
			Object[] arguments, String[] operators) throws DBAppException {
		ArrayList<Hashtable<String, Object>> output = new ArrayList<Hashtable<String, Object>>();

		/**
		 * TODO defensive checks to ensure table exists and column exists
		 */

		String primaryKey = getPrimaryKeyColumnName(tableName);

		ArrayList<String> indexedColumns = getIndexedColumns(tableName);

		if (columnName.equals(primaryKey)) {
			if (indexedColumns.contains(columnName)) {
				// indexed and primary key
			} else {
				// primary key only
			}
		} else {
			if (indexedColumns.contains(columnName)) {
				// indexed and not primary key
			} else {
				// not primary key and not indexed
			}
		}

		return output.iterator();
	}

	// searches the BRIN index and returns the numbers of the pages that
	// satisfied the conditions of the query.
	// These pages may be table pages or dense index pages
	// it all depends on whether we are answering a primary key query or not.
	protected static ArrayList<Integer> getSatisfyingPagesFromBrinIndex(String tableName, String columnName,
			Object[] arguments, String[] operators) throws DBAppException {
		String brinIndexPath = "data/" + tableName + "/" + columnName + "indices/BRIN/";
		ArrayList<Integer> satisfyingPageNumbers = new ArrayList<Integer>();

		int brinPageNumber = 1;

		while (true) {
			Page brinPage = PageManager.loadPageIfExists(brinIndexPath + "page_" + brinPageNumber + ".ser");
			if (brinPage == null)
				break;
			for (int i = 0; i < brinPage.getRows().length; i++) {
				Hashtable<String, Object> currentRow = brinPage.getRows()[i];
				if (brinEntrySatisfiesConditions(currentRow.get(columnName + "Max"), currentRow.get(columnName + "Min"),
						arguments, operators) && !((boolean) currentRow.get("isDeleted"))) {
					satisfyingPageNumbers.add((Integer) currentRow.get("pageNumber"));
				}
			}
			brinPageNumber++;
		}

		return satisfyingPageNumbers;
	}

	// checks if the range in a BRIN index entry satisfies the query conditions
	protected static boolean brinEntrySatisfiesConditions(Object brinMax, Object brinMin, Object[] arguments,
			String[] operators) throws DBAppException {

		for (int i = 0; i < operators.length; i++) {

			switch (operators[i]) {
			case "<":
				if (!compareColumnToArgumentUsingOperator(brinMin, arguments[i], "<")) {
					return false;
				}
				break;

			case ">":
				if (!compareColumnToArgumentUsingOperator(brinMax, arguments[i], ">")) {
					return false;
				}
				break;

			case "<=":
				if (compareColumnToArgumentUsingOperator(brinMin, arguments[i], ">")) {
					return false;
				}
				break;

			case ">=":
				if (compareColumnToArgumentUsingOperator(brinMax, arguments[i], "<")) {
					return false;
				}
				break;
			}
		}

		return true;
	}

	// loads the contents of all the pages of the BRIN index into an array list
	// and returns it
	protected static ArrayList<Page> loadAllBrinPages(String tableName, String columnName) {
		ArrayList<Page> brinPages = new ArrayList<Page>();

		String brinIndexPath = "data/" + tableName + "/" + columnName + "indices/BRIN/";

		int pageCounter = 1;

		while (true) {
			Page currentPage = PageManager.loadPageIfExists(brinIndexPath + "page_" + pageCounter + ".ser");

			if (currentPage == null)
				break;

			brinPages.add(currentPage);

			pageCounter++;
		}

		return brinPages;
	}

	// compares a column's value to an argument using an operator
	// returns the boolean result of the comparison
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected static boolean compareColumnToArgumentUsingOperator(Object columnValue, Object argument, String operator)
			throws DBAppException {
		boolean result = false;

		Comparable firstArgument = (Comparable) columnValue;
		Comparable secondArgument = (Comparable) argument;

		switch (operator) {
		case ">":
			result = firstArgument.compareTo(secondArgument) > 0;
			break;
		case ">=":
			result = firstArgument.compareTo(secondArgument) >= 0;
			break;
		case "<":
			result = firstArgument.compareTo(secondArgument) < 0;
			break;
		case "<=":
			result = firstArgument.compareTo(secondArgument) <= 0;
			break;
		default:
			throw new DBAppException("Invalid operator during selection.");
		}

		return result;
	}

	// returns the column name of the primary key
	protected static String getPrimaryKeyColumnName(String tableName) {
		String line = null;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader("data/metadata.csv"));
			line = br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}

		String primaryKey = null;

		while (line != null) {
			String[] content = line.split(",");

			if (content[0].equals(tableName)) {
				if ((content[3].toLowerCase()).equals("true"))
					primaryKey = content[1];
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
		return primaryKey;
	}

	// checks what columns are indexed and returns their names
	protected static ArrayList<String> getIndexedColumns(String tableName) {

		// the array list to return
		ArrayList<String> indexedColumns = new ArrayList<>();

		String line = null;
		BufferedReader br = null;

		try {
			br = new BufferedReader(new FileReader("data/metadata.csv"));
			line = br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}

		while (line != null) {
			String[] content = line.split(",");

			if (content[0].equals(tableName)) {
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

		return indexedColumns;
	}

	// Selects From Non-Indexed Column
	protected static Iterator<Hashtable<String, Object>> selectFromNonIndexedColumn(String strTableName,
			String strColumnName, Object[] objarrValues, String[] strarrOperators) throws DBAppException {

		ArrayList<Hashtable<String, Object>> output = new ArrayList<Hashtable<String, Object>>();
		int tablePageNumber = 1;

		while (true) {

			Page page = PageManager.loadPageIfExists("data/" + strTableName + "/page_" + tablePageNumber++ + ".ser");

			if (page == null)
				break;

			for (int i = 0; i < page.getMaxRows() && page.getRows()[i] != null; i++)
				if (isValueInResultSet(page.getRows()[i].get(strColumnName), objarrValues, strarrOperators)
						&& ((boolean) page.getRows()[i].get("isDeleted") == false))
					output.add(page.getRows()[i]);

		}

		return output.iterator();

	}

	// Checks Whether Value Satisfies All Conditions
	protected static boolean isValueInResultSet(Object columnValue, Object[] objarrValues, String[] strarrOperators)
			throws DBAppException {

		return isValueInResultSet(columnValue, objarrValues, strarrOperators, 0);

	}

	protected static boolean isValueInResultSet(Object columnValue, Object[] objarrValues, String[] strarrOperators,
			int index) throws DBAppException {

		if (index == objarrValues.length)
			return true;
		else
			return compareColumnToArgumentUsingOperator(columnValue, objarrValues[index], strarrOperators[index])
					&& isValueInResultSet(columnValue, objarrValues, strarrOperators, ++index);

	}

	// Checks From Dense Indexed Column
	protected static Iterator<Hashtable<String, Object>> denseIndexRetrieval(String strTableName, String strColumnName,
			Object[] objarrValues, String[] strarrOperators, ArrayList<Integer> denseIndexPageNumbers)
			throws DBAppException {

		ArrayList<Hashtable<String, Object>> output = new ArrayList<Hashtable<String, Object>>();

		for (int denseIndexPageNumber : denseIndexPageNumbers) {

			Page page = PageManager.loadPageIfExists("data/" + strTableName + "/" + strColumnName
					+ "/indices/Dense/page_" + denseIndexPageNumber + ".ser");

			if (page == null)
				break;

			for (int i = 0; i < page.getMaxRows() && page.getRows()[i] != null; i++)
				if (isValueInResultSet(page.getRows()[i].get(strColumnName), objarrValues, strarrOperators)
						&& ((boolean) page.getRows()[i].get("isDeleted") == false)) {

					Page tempPage = PageManager.loadPageIfExists(
							"data/" + strTableName + "/page_" + ((int) page.getRows()[i].get("pageNumber")) + ".ser");
					Hashtable<String, Object> tuple = tempPage.getRows()[((int) page.getRows()[i].get("locInPage"))];

					output.add(tuple);

				}

		}

		return output.iterator();

	}

}
