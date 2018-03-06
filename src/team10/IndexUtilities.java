package team10;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;

public class IndexUtilities {

	// Checks if the directory of table exists
	protected static boolean tableDirectoryExists(String strTableName) throws DBAppException {
		File tableFile = new File("data/" + strTableName);
		if (tableFile.exists() && tableFile.isDirectory()) {
			return true;
		}
		return false;
	}

	// Checks in the meta data whether the column represents a primary key or not
	protected static boolean isColumnPrimary(String columnMeta) throws DBAppException {
		if (columnMeta == null) {
			throw new DBAppException("meta data retreival error");
		}
		String[] columnParams = columnMeta.split(",");
		if (columnParams[3].equals("true")) {
			return true;
		}
		return false;

	}

	// Retrieves meta data of specific column in specific table
	protected static String retrieveColumnMetaInTable(String strTableName, String strColumnName) {
		BufferedReader metaReader;
		try {
			metaReader = new BufferedReader(new FileReader("data/metadata.csv"));
			String metaLine = metaReader.readLine();
			while (metaLine != null) {
				String[] tableParams = metaLine.split(",");
				if (tableParams[0].equals(strTableName) && tableParams[1].equals(strColumnName)) {
					metaReader.close();
					return metaLine;
				}
				metaLine = metaReader.readLine();
			}

		} catch (FileNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
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
			if (name.substring(0, 6).equals("dense_") && name.substring(name.indexOf('.')).equals(".ser"))
				pages.add(PageManager.deserializePage(file.getPath()));

		}

		return pages;

	}

	// check a file path and create directories that don't exist through the file path on the file system
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