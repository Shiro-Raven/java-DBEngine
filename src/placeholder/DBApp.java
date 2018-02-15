package placeholder;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;

public class DBApp {
	public void init() {

	}

	// TODO change Exception to DBAppException
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
			// TODO business logic table creation
			CreationUtilities.addDirectory(strTableName, "data");
			try {
				CreationUtilities.addMetaData(strTableName, strClusteringKeyColumn, htblColNameType);
			} catch (IOException e) {
				e.printStackTrace();
			}
		} else {
			throw new DBAppException();
		}
	}

	// Skeleton of InsertIntoTaple
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
				if (content[3].equals("true"))
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

}
