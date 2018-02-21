package team10;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;
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
		ArrayList<String> primary_key = new ArrayList<>();
		Hashtable<String, String> ColNameType = new Hashtable<>();

		while (line != null) {
			String[] content = line.split(",");

			if (content[0].equals(strTableName)) {
				data.add(content);
				ColNameType.put(content[1], content[2]);
				if ((content[3].toLowerCase()).equals("true"))
					primary_key.add(content[1]);
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

		for (String key : primary_key)
			if (htblColNameValue.get(key).equals(null))
				throw new DBAppException("Primary Key Can NOT be null");

		if (!InsertionUtilities.isValidTuple(ColNameType, htblColNameValue))
			throw new DBAppException(
					"The tuple you're trying to insert into table " + strTableName + " is not a valid tuple!");

		int[] positionToInsertAt = InsertionUtilities.searchForInsertionPosition(strTableName, primary_key,
				htblColNameValue);
		try {
			InsertionUtilities.insertTuple(strTableName, positionToInsertAt, htblColNameValue);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException {

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
		for (String tableKey: tableKeys) {
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

}
