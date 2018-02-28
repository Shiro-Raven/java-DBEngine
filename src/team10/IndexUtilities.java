package team10;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

public class IndexUtilities {

	// Checks if the directory of table exists
	protected static boolean tableDirectoryExists(String strTableName) throws DBAppException {

		return false;
	}

	// Checks in the meta data whether the column is primary or not
	protected static boolean isColumnPrimary(String columnMeta) {

		return false;

	}

	// Retrieves meta data of specific column in specific table
	protected static String retrieveColumnInTable(String strTableName, String strColumnName) {

		return null;

	}

	// BRIN index business logic for now
	protected static void createBRINFiles(String strTableName, String strColumnName, boolean isPrimary) {

	}

	// Creates dense index of the given column in the given table
	protected static void createDenseIndex(String strTableName, String strColumnName) {

	}

	// Retrieve all pages in a given path
	protected static ArrayList<Page> getAllTablePages(String filepath) throws IOException, ClassNotFoundException {

		IndexUtilities.validateDirectory(filepath);
		ArrayList<Page> pages = new ArrayList<Page>();
		File files = new File(filepath);

		for (File file : files.listFiles()) {
			
			String name = file.getName();
			if(name.substring(0, 6).equals("dense_") && name.substring(name.indexOf('.')).equals(".ser"))
				pages.add(PageManager.deserializePage(file.getPath()));
			
		}

		return pages;

	}

	// Checks a directory exists
	protected static void validateDirectory(String filepath) throws IOException {

		String[] pathParams = filepath.split("/");
		filepath = "";
		int i = 0;

		do {

			filepath += (i == 0) ? pathParams[i++] : "/" + pathParams[i++];

			File file = new File(filepath);

			if (!file.exists())
				file.mkdir();

		} while (i < pathParams.length);

	}

}