package placeholder;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.Hashtable;

public class DBApp {
	public void init() {

	}

	// TODO change Exception to DBAppException
	public void createTable(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) throws Exception {
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
		// TODO implement Name validation check
		// A more robust approach would be check the directories and metadata file at
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
		// TODO implement Key validation check
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

	private static boolean checkMeta() {
		// TODO implement meta data check
		File metaFile = new File("data/metadata.csv");
		if (metaFile.exists()) {
			return true;
		}
		return false;	
	}

	private static void createMeta() throws IOException {
		File metaFile = new File("data/metadata.csv");
		metaFile.createNewFile();
	}

	private static void addMetaData(String strTableName, String strClusteringKeyColumn,
			Hashtable<String, String> htblColNameType) {
		// TODO add meta data of table to file
	}

	private static void addDirectory(String directoryName, String path) throws Exception {
		File directory = new File(path + "/" + directoryName);
		// TODO change Exception to DBAppException
		if (!directory.mkdir()) {
			throw new Exception();
		}

	}

	// main method for tests
	public static void main(String[] args) throws Exception {
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

		// print false in case a directory with the same name exists&& true otherwise
		// System.out.println(checkValidName("testTable"));

		// tests for createMeta

		// createMeta();// create a new metadata.csv file or replace in case the file
		// exists(this case shouldn't happen because of checkMeta condition)
		
		//tests for checkVaidKeys
		
		//failing case, type unsupported
		
		//Hashtable failedTable = new Hashtable( );
		//failedTable.put("id", "java.lang.Integer");
		//failedTable.put("name", "unsupported.Type");
		//failedTable.put("gpa", "java.lang.Double"); 
		//System.out.println(checkValidKeys(failedTable));
		
		//successful case, types are supported
		
		//Hashtable successfulTable = new Hashtable();
		//successfulTable.put("name","java.lang.String");
		//successfulTable.put("email", "java.lang.String");
		//successfulTable.put("age", "java.lang.Integer");
		//successfulTable.put("favoriteDouble", "java.lang.Double");
		//successfulTable.put("DateOfBirth", "java.util.Date");
		//successfulTable.put("Married","java.lang.Boolean");
		//System.out.println(checkValidKeys(successfulTable));
		
		//tests for 
	}

}
