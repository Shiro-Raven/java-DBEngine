package placeholder;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.Hashtable;

public class DBApp {
	public void init() {

	}

	// TODO change Exception to DBAppException
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws Exception {
		if (strTableName == null || strClusteringKeyColumn == null || htblColNameType == null) {
			throw new Exception();
		}
		if (!checkMeta()) {
			createMeta();
		}
		if (checkValidName(strTableName) && checkValidKeys(htblColNameType)) {
			// TODO business logic table creation
			addDirectory(strTableName, "data");
			addMetaData(strTableName, strClusteringKeyColumn, htblColNameType);
		} else {
			throw new Exception();
		}
	}

	private static boolean checkValidName(String strTableName) {
		// A more robust approach would be check the directories and metadata
		// file at
		// the same time
		File dataDirectory = new File("data");
		File[] fileList = dataDirectory.listFiles();
		ArrayList<String> tableNames = new ArrayList<String>();
		for (File file : fileList) {
			if (file.isDirectory()) {
				tableNames.add(file.getName());
			}
		}
		if (tableNames.contains(strTableName)) {
			return false;
		} else {
			return true;
		}

	}

	private static boolean checkValidKeys(Hashtable<String, String> htblColNameType) {
		String[] supportedTypes = { "java.lang.Integer", "java.lang.String", "java.lang.Double", "java.lang.Boolean",
				"java.util.Date" };
		// get all the keys from the hashtable
		Enumeration<String> htblKeys = htblColNameType.keys();
		// iterate over the keys
		while (htblKeys.hasMoreElements()) {
			String key = htblKeys.nextElement();
			// if a type is not supported return false
			if (!arrayContains(supportedTypes, htblColNameType.get(key)))
				return false;
		}
		// if all types are supported, passed validation
		return true;
	}

	private static boolean arrayContains(String[] array, String key) {
		for (String element : array) {
			if (key.equals(element))
				return true;
		}
		return false;
	}

	// check metadata.csv file exists
	private static boolean checkMeta() {
		File metaFile = new File("data/metadata.csv");
		if (metaFile.exists()) {
			return true;
		}
		return false;
	}

	// create a metadata.csv file
	private static void createMeta() throws IOException {
		File metaFile = new File("data/metadata.csv");
		metaFile.createNewFile();
	}

	// add meta data of table to metadaata.csv file
	private static void addMetaData(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws Exception {
		if (!checkValidClusteringKey(strClusteringKeyColumn, htblColNameType)) {
			// TODO change exception to DBAppException
			throw new Exception();
		}
		Enumeration<String> htblKeys = htblColNameType.keys();
		FileWriter metaWriter = new FileWriter("data/metadata.csv", true);
		while (htblKeys.hasMoreElements()) {
			String name = htblKeys.nextElement();
			// TODO add business logic with the implementation of indexing
			String isIndexed = "false";
			String isClusteringKey = "";
			if (name.equals(strClusteringKeyColumn))
				isClusteringKey = "true";
			else
				isClusteringKey = "false";
			String type = htblColNameType.get(name);
			metaWriter.write(strTableName + "," + name + "," + type + "," + isClusteringKey + "," + isIndexed + "\n");

		}
		metaWriter.close();
	}

	// check whether the clustering key exists in the table declaration
	private static boolean checkValidClusteringKey(String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) {
		Enumeration<String> htblKeys = htblColNameType.keys();
		while (htblKeys.hasMoreElements()) {
			String key = htblKeys.nextElement();
			if (key.equals(strClusteringKeyColumn)) {
				return true;
			}
		}
		return false;
	}

	// add a new Directory
	private static void addDirectory(String directoryName, String path) throws Exception {
		File directory = new File(path + "/" + directoryName);
		// TODO change Exception to DBAppException
		if (!directory.mkdir()) {
			throw new Exception();
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

	// main method for tests
	// public static void main(String[] args) throws Exception {
	// tests for directory insertion

	// test for first insertion of directory
	// addDirectory("hello","data");
	// test for duplicate insertion of directory
	// addDirectory("hello","data");//Exception generated
	// more reasons might cause the failure of directory insertion
	// test for add directory to a non-existent parent
	// addDirectory("hello","foo");//Exception Generated

	// tests for meta data file existence check

	// in case meta data file exists print true, otherwise false
	// System.out.println(checkMeta());

	// tests for checkValidName

	// print false in case a directory with the same name exists&& true
	// otherwise
	// System.out.println(checkValidName("testTable"));

	// tests for createMeta

	// createMeta();// create a new metadata.csv file or replace in case the
	// file
	// exists(this case shouldn't happen because of checkMeta condition)

	// tests for checkVaidKeys

	// failing case, type unsupported

	// Hashtable failedTable = new Hashtable( );
	// failedTable.put("id", "java.lang.Integer");
	// failedTable.put("name", "unsupported.Type");
	// failedTable.put("gpa", "java.lang.Double");
	// System.out.println(checkValidKeys(failedTable));

	// successful case, types are supported

	// Hashtable successfulTable = new Hashtable();
	// successfulTable.put("name","java.lang.String");
	// successfulTable.put("email", "java.lang.String");
	// successfulTable.put("age", "java.lang.Integer");
	// successfulTable.put("favoriteDouble", "java.lang.Double");
	// successfulTable.put("DateOfBirth", "java.util.Date");
	// successfulTable.put("Married","java.lang.Boolean");
	// System.out.println(checkValidKeys(successfulTable));

	// tests for addMetaData

	/*
	 * accessing this method publicly causes unwanted behavior to be accessed
	 * only as part of the business logic in create Table method
	 */
	// Hashtable<String, String> metaTable1 = new Hashtable<String, String>();
	// metaTable1.put("id", "java.lang.Integer");
	// metaTable1.put("testString", "java.lang.String");
	// metaTable1.put("testDouble", "java.lang.Double");
	// metaTable1.put("testBoolean", "java.lang.Boolean");
	// metaTable1.put("testDate", "java.util.Date");
	// addMetaData("testTable5", "id", metaTable1);//Generates exception if
	// clustering key doesn't exist in Hashtable

	// }

}
