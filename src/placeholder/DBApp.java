package placeholder;

import java.io.File;
import java.util.Hashtable;

public class DBApp {
	public void init() {

	}

	// TODO change Exception to DBAppException
	public void createTable(String strTableName, Hashtable<String, String> htblColNameType) throws Exception {
		if(checkMeta())
		if (checkValidName(strTableName) && checkValidKeys(htblColNameType)) {
			// TODO business logic table creation
			addDirectory(strTableName,"data");
			addMetaData(strTableName,htblColNameType);
		} else {
			throw new Exception();
		}
	}

	private static boolean checkValidName(String strTableName) {
		//TODO implement Name validation check
		return true;
	}

	private static boolean checkValidKeys(Hashtable<String, String> htblColNameType) {
		//TODO implement Key validation check
		return true;
	}
	private static boolean checkMeta() {
		//TODO implement meta data check
		return true;
	}
	private static void addMetaData(String strTableName,Hashtable<String, String> htblColNameType) {
		//TODO add meta data of table to file
	}
	private static void addDirectory(String directoryName, String path) throws Exception {
		//TODO add directory for table pages
		File directory = new File(path+"/"+directoryName);
		//TODO change Exception to DBAppException
		if(!directory.mkdir()) {
			throw new Exception();
		}
		
	}
	//main method for tests
	public static void main (String [] args) throws Exception {
		//tests for directory insertion
		
			//test for first insertion of directory 
			//addDirectory("hello","data");
			//test for duplicate insertion of directory
			//addDirectory("hello","data");//Exception generated
			//more reasons might cause the failure of directory insertion
			//test for add directory to a non-existent parent
			//addDirectory("hello","foo");//Exception Generated
		
	}
	
}
