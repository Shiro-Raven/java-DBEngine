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

	// Get a page based on the containing directory path
	// Throws a DBAppException in case the file path does not point to a directory
	// Throws a DBAppException in case the page does not exist
	protected static Page retreivePage(String pageDirectoryPath, int pageNumber) throws DBAppException {
		File pageDirectory = new File(pageDirectoryPath);
		if (!pageDirectory.exists()) {
			throw new DBAppException("The file path supplied does not exist");
		}
		if (!pageDirectory.isDirectory()) {
			throw new DBAppException("The file path supplied to retrieve the page is not a directory");
		}
		File pageFile = new File(pageDirectoryPath + "page_" + pageNumber + ".ser");
		if (!pageFile.exists()) {
			throw new DBAppException("The page file does not exist");
		}
		try {
			return PageManager.deserializePage(pageFile.getPath());
		} catch (ClassNotFoundException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return null;

	}

	// Retrieve all pages in a given path
	protected static ArrayList<Page> retreiveAllPages(String filepath) throws IOException, ClassNotFoundException {

		IndexUtilities.validateDirectory(filepath);
		ArrayList<Page> pages = new ArrayList<Page>();
		File files = new File(filepath);

		for (File file : files.listFiles()) {

			String name = file.getName();
			if (name.startsWith("dense_") && name.endsWith(".ser"))
				pages.add(PageManager.deserializePage(file.getPath()));

		}

		return pages;

	}

	// check a file path and create directories that don't exist through the file
	// path on the file system
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