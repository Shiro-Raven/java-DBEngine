package team10;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Set;

public class DBApp {
	public void init() {

	}

	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws DBAppException {
		if (strTableName == null || strClusteringKeyColumn == null || htblColNameType == null) {
			throw new DBAppException();
		}
		if (!CreationUtilities.checkMeta()) {
			try {
				CreationUtilities.createMeta();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		if (CreationUtilities.checkValidName(strTableName) && CreationUtilities.checkValidKeys(htblColNameType)) {
			try {
				CreationUtilities.addMetaData(strTableName, strClusteringKeyColumn, htblColNameType);
			} catch (IOException e) {
				e.printStackTrace();
			}
			CreationUtilities.addDirectory(strTableName, "data");
		} else {
			throw new DBAppException();
		}
		System.out.println("Table Created!");
	}

	public void insertIntoTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
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
			InsertionUtilities.insertTuple(strTableName, positionToInsertAt, htblColNameValue, true);
		} catch (IOException e) {
			e.printStackTrace();
		}

		ArrayList<Integer> changedPagesAfterDenseIndexUpdate = new ArrayList<Integer>();

		for (int i = 0; i < indexedColumns.size(); i++) {
			if (!indexedColumns.get(i).equals(primaryKey)) {
				changedPagesAfterDenseIndexUpdate = InsertionUtilities.updateDenseIndexAfterInsertion(strTableName,
						indexedColumns.get(i), tempPositionToInsertAt[0], tempPositionToInsertAt[1],
						htblColNameValue.get(indexedColumns.get(i)));
				try {
					IndexUtilities.updateBRINIndexOnDense(strTableName, indexedColumns.get(i),
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

		}

		/** TODO update the BRIN index after insertion **/

		System.out.println("Tuple Inserted!");
		System.out.println(
				"Changed Dense Index Page Numbers at the end: " + changedPagesAfterDenseIndexUpdate.toString());

	}

	@SuppressWarnings("unchecked")
	public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, ParseException {
		// Check if null values in general
		if (strTableName == null || strKey == null || htblColNameValue == null)
			throw new DBAppException("Do not leave stuff null!");

		// Check if all values are null
		if (!UpdateUtilities.checkNotAllNulls(htblColNameValue))
			throw new DBAppException("You want to update to nulls, huh?");

		// Check if the table exists using checkValidName from CreationUtilites
		if (CreationUtilities.checkValidName(strTableName))
			throw new DBAppException("Do you know your tables?");

		// Get Columns and Primary key of needed table (with its type)
		ArrayList<Object> neededData = UpdateUtilities.getColumnsAndKey(strTableName);

		Hashtable<String, String> tblNameType = (Hashtable<String, String>) neededData.get(0);
		String PKeyName = (String) neededData.get(1);

		// Check if valid tuple
		if (!InsertionUtilities.isValidTuple(tblNameType, htblColNameValue))
			throw new DBAppException("Stick to to the types!");

		Object keyValue = null;
		// Cast key to its type (I am assuming no idiot would use Boolean as a
		// type for the primary key)
		switch (tblNameType.get(PKeyName)) {
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

		int currentPgNo = 1;
		Page currentPage;
		boolean done = false;
		// and finally, update the table
		while (true) {
			try {
				// Load Page
				currentPage = PageManager
						.deserializePage("data/" + strTableName + "/" + "page_" + currentPgNo + ".ser");
				// Go through page
				for (int i = 0; i < currentPage.getRows().length; i++) {
					Hashtable<String, Object> curRow = currentPage.getRows()[i];
					// if a matching row is found
					if (curRow.get(PKeyName).equals(keyValue) && !((boolean) curRow.get("isDeleted"))) {
						// Check if the primary key is being changed
						if (htblColNameValue.get(PKeyName) != null) {
							// If yes, check that the new value is not already
							// used somewhere
							Object newValue = htblColNameValue.get(PKeyName);
							if (UpdateUtilities.checkNotUsed(strTableName, newValue, PKeyName)) {
								// Update the tuple
								for (String key : tblNameType.keySet()) {
									if (htblColNameValue.containsKey(key))
										curRow.put(key, htblColNameValue.get(key));
								}
								// Store it
								Hashtable<String, Object> newTuple = new Hashtable<>();
								for (String key : curRow.keySet()) {
									newTuple.put(key, curRow.get(key));
								}
								// Delete it
								curRow.put("isDeleted", true);
								PageManager.serializePage(currentPage,
										"data/" + strTableName + "/" + "page_" + currentPgNo + ".ser");
								// re-insert it to keep table sorted
								insertIntoTable(strTableName, newTuple);
								done = true;
								break;
							} else {
								throw new DBAppException("Primary key value used somewhere.");
							}
						}
						// if not, just update the table
						else {
							for (String key : tblNameType.keySet()) {
								if (!key.equals(PKeyName) && htblColNameValue.containsKey(key))
									curRow.put(key, htblColNameValue.get(key));
							}
							PageManager.serializePage(currentPage,
									"data/" + strTableName + "/" + "page_" + currentPgNo + ".ser");
							done = true;
							break;
						}
					}
				}
				if (done)
					break;
				currentPgNo++;
			} catch (Exception e) {
				// e.printStackTrace();
				// No more pages and the row to be update still not found
				throw new DBAppException("You are trying to update a non-existing row!");
			}
		}
		System.out.println("Update made successfully!");
	}

	public void deleteFromTable(String strTableName, Hashtable<String, Object> htblColNameValue) throws DBAppException {
		if (strTableName == null)
			throw new DBAppException("No table name provided!");

		if (htblColNameValue == null)
			throw new DBAppException("No record provided!");

		if (CreationUtilities.checkValidName(strTableName))
			throw new DBAppException("Invalid table name!");

		String line = null;
		BufferedReader br = null;
		Hashtable<String, String> colNameType = new Hashtable<>();
		String primaryKey = null;

		try {
			br = new BufferedReader(new FileReader("data/metadata.csv"));
			line = br.readLine();

			while (line != null) {
				String[] content = line.split(",");
				if (content[0].equals(strTableName)) {
					colNameType.put(content[1], content[2]);
					if ((content[3].toLowerCase()).equals("true"))
						primaryKey = content[1];
				}
				line = br.readLine();
			}

			br.close();
		} catch (IOException e) {
			e.printStackTrace();
			return;
		}

		if (!InsertionUtilities.isValidTuple(colNameType, htblColNameValue))
			throw new DBAppException(
					"The tuple you're trying to delete from table " + strTableName + " is not a valid tuple!");

		Set<String> tableKeys = colNameType.keySet();

		// all nulls throw exception
		// assumption, subject to change after asking the doctor
		boolean allNull = true;
		for (String tableKey : tableKeys) {
			if (htblColNameValue.get(tableKey) != null) {
				allNull = false;
				break;
			}
		}
		if (allNull)
			throw new DBAppException("All null values tuple!");
		// end of assumption

		try {
			DeletionUtilities.deleteTuples(strTableName, htblColNameValue, primaryKey, tableKeys);
		} catch (IOException e) {
			e.printStackTrace();
		}

	}

	public void createBRINIndex(String strTableName, String strColumnName) throws DBAppException {

	}

	@SuppressWarnings("rawtypes")
	public Iterator selectFromTable(String strTableName, String strColumnName, Object[] objarrValues,
			String[] strarrOperators) throws DBAppException {
		return SelectionUtilities.selectFromTableHelper(strTableName, strColumnName, objarrValues, strarrOperators);
	}
}
