package team10;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;

public class SelectionUtilities {

	// checks if the arguments are of the same type as the table's column
	protected static boolean areValidArguments(Object[] arguments, String columnType) {
		for (int i = 0; i < arguments.length; i++) {
			if (!(arguments[i].getClass().toString().substring(6)).equals(columnType))
				return false;
		}
		return true;
	}

	// the main method for the select functionality
	// according to the column we select on, whether it is the primary key, and
	// whether it is indexed, the path of execution is chosen
	protected static Iterator<Hashtable<String, Object>> selectFromTableHelper(String tableName, String columnName,
			Object[] arguments, String[] operators) throws DBAppException {

		// check if table and column exist
		String columnMetaDataLine = IndexUtilities.retrieveColumnMetaInTable(tableName, columnName);

		if (columnMetaDataLine == null)
			throw new DBAppException("The table or column you entered does not exist.");

		// check if all arguments are valid
		String[] columnMetaData = columnMetaDataLine.split(",");

		String columnType = columnMetaData[2];

		if (!areValidArguments(arguments, columnType)) {
			throw new DBAppException("The values you entered for comparison did not match with the column's type.");
		}

		// incorrect conditions
		if (arguments.length != operators.length)
			throw new DBAppException("The number of operators does not match the number of arguments.");

		// there are no conditions; return everything
		if (operators.length == 0) {
			return selectFromNonIndexedColumn(tableName, columnName, arguments, operators);
		}

		// there are conditions
		String primaryKey = getPrimaryKeyColumnName(tableName);

		ArrayList<String> indexedColumns = getIndexedColumns(tableName);

		if (columnName.equals(primaryKey)) {
			if (indexedColumns.contains(columnName)) {
				// indexed and primary key
				return selectByPrimaryKeyIndexed(tableName, columnName, arguments, operators);
			} else {
				// primary key only
				return selectFromNonIndexedColumn(tableName, columnName, arguments, operators);
			}
		} else {
			if (indexedColumns.contains(columnName)) {
				// indexed and not primary key
				return selectByNonPrimaryKeyIndexed(tableName, columnName, arguments, operators);
			} else {
				// not primary key and not indexed
				return selectFromNonIndexedColumn(tableName, columnName, arguments, operators);
			}
		}
	}

	// handles selection for primary key indexed case
	protected static Iterator<Hashtable<String, Object>> selectByPrimaryKeyIndexed(String tableName, String columnName,
			Object[] arguments, String[] operators) throws DBAppException {

		ArrayList<Integer> satisfyingTablePages = getSatisfyingPagesFromBrinIndex(tableName, columnName, arguments,
				operators);

		return SelectionUtilities.primaryKeyIndexedRetrieval(tableName, columnName, arguments, operators,
				satisfyingTablePages);
	}

	// handles selection for non-primary key indexed case
	protected static Iterator<Hashtable<String, Object>> selectByNonPrimaryKeyIndexed(String tableName,
			String columnName, Object[] arguments, String[] operators) throws DBAppException {

		ArrayList<Integer> satisfyingTablePages = getSatisfyingPagesFromBrinIndex(tableName, columnName, arguments,
				operators);

		return denseIndexRetrieval(tableName, columnName, arguments, operators, satisfyingTablePages);

	}

	// searches the BRIN index and returns the numbers of the pages that
	// satisfied the conditions of the query.
	// These pages may be table pages or dense index pages
	// it all depends on whether we are answering a primary key query or not.
	protected static ArrayList<Integer> getSatisfyingPagesFromBrinIndex(String tableName, String columnName,
			Object[] arguments, String[] operators) throws DBAppException {
		String brinIndexPath = "data/" + tableName + "/" + columnName + "/indices/BRIN/";
		ArrayList<Integer> satisfyingPageNumbers = new ArrayList<Integer>();

		int brinPageNumber = 1;

		while (true) {
			Page brinPage = PageManager.loadPageIfExists(brinIndexPath + "page_" + brinPageNumber + ".ser");
			
			if (brinPage == null)
				break;
			for (int i = 0; i < brinPage.getRows().length && brinPage.getRows()[i] != null; i++) {
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

	// Selects From Non-Indexed Column
	protected static Iterator<Hashtable<String, Object>> primaryKeyIndexedRetrieval(String strTableName,
			String strColumnName, Object[] objarrValues, String[] strarrOperators, ArrayList<Integer> tablePageNumbers)
			throws DBAppException {

		ArrayList<Hashtable<String, Object>> output = new ArrayList<Hashtable<String, Object>>();

		for (int tablePageNumber : tablePageNumbers) {

			Page page = PageManager.loadPageIfExists("data/" + strTableName + "/page_" + tablePageNumber + ".ser");

			if (page == null)
				break;

			for (int i = 0; i < page.getMaxRows() && page.getRows()[i] != null; i++)
				if (isValueInResultSet(page.getRows()[i].get(strColumnName), objarrValues, strarrOperators)
						&& ((boolean) page.getRows()[i].get("isDeleted") == false))
					output.add(page.getRows()[i]);

		}

		return output.iterator();

	}

}
