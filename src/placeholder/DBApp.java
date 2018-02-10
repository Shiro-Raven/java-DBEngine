package placeholder;

import java.util.Hashtable;

public class DBApp {
	public void init() {

	}

	// TODO change Exception to DBAppException
	public void createTable(String strTableName, Hashtable<String, String> htblColNameType) throws Exception {
		if(checkMeta())
		if (checkValidName(strTableName) && checkValidKeys(htblColNameType)) {
			// TODO business logic table creation
			addMetaData(strTableName,htblColNameType);
			addDirectory(strTableName,"data");
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
	private static void addDirectory(String directoryName, String filePath) {
		//TODO add directory for table pages
	}
	
	public static void main (String [] args) {
		
	}
	
}
