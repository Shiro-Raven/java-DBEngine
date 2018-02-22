package team10;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Hashtable;

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

	@SuppressWarnings("unchecked")
	public void updateTable(String strTableName, String strKey, Hashtable<String, Object> htblColNameValue)
			throws DBAppException, ParseException {
		// Check if null values in general
		if (strTableName == null || strKey == null || htblColNameValue == null)
			throw new DBAppException("Do not leave stuff null! Bitch!");

		// Check if all values are null
		if (!UpdateUtilities.checkNotAllNulls(htblColNameValue))
			throw new DBAppException("You want to update to nulls, huh? Bitch?");

		// Check if the table exists using checkValidName from CreationUtilites
		if (CreationUtilities.checkValidName(strTableName))
			throw new DBAppException("Do you know your tables? Bitch?");

		// Get Columns and Primary key of needed table (with its type)
		ArrayList<Object> neededData = UpdateUtilities.getColumnsAndKey(strTableName);
		
		Hashtable<String,String> tblNameType = (Hashtable<String, String>) neededData.get(0);
		String PKeyName = (String) neededData.get(1);

		// Check if valid tuple
		if (!InsertionUtilities.isValidTuple(tblNameType, htblColNameValue))
			throw new DBAppException("Stick to to the types, bitch!");

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
					if(curRow.get(strKey).equals(keyValue) && !((boolean)curRow.get("isDeleted"))){
						// Check if the primary key is being changed
						if(htblColNameValue.get(PKeyName) != null){
							// If yes, check that the new value is not already used somewhere
							Object newValue = htblColNameValue.get(PKeyName);
							if(UpdateUtilities.checkNotUsed(strTableName,newValue, PKeyName)){
								// Update the tuple
								for(String key : tblNameType.keySet()){
									if(!key.equals(PKeyName) && htblColNameValue.containsKey(key))
										curRow.put(key, htblColNameValue.get(key));
								}
								// Store it
								Hashtable<String, Object> newTuple = curRow;
								// Delete it
								curRow.put("isDeleted", true);
								// re-insert it to keep table sorted
								insertIntoTable(strTableName, newTuple);
								done = true;
								break;
							}
						}
						// if not, just update the table
						else{
							for(String key : tblNameType.keySet()){
								if(!key.equals(PKeyName) && htblColNameValue.containsKey(key))
									curRow.put(key, htblColNameValue.get(key));
							}
							done = true;
							break;
						}
					}
				}
				PageManager.serializePage(currentPage, "data/" + strTableName + "/" + "page_" + currentPgNo + ".ser");
				if(done) break;
				currentPgNo++;
			} catch (Exception e) {
				// No more pages and the row to be update still not found
				throw new DBAppException("You are trying to update a non existing row, bitch!");
			}
		}
		System.out.println("Update made successfully!");
	}
}
