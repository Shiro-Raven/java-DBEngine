package team10;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Hashtable;
import java.util.Set;

public class DeletionUtilities {
	public static void deleteTuples(String strTableName, Hashtable<String, Object> htblColNameValue, String primaryKey,
			Set<String> tableKeys) throws IOException {
		int deleteCount = 0; // keeps track of the no. of deleted rows
		int pageNumber = 1;
		Page currentPage = null;

		// load and process table pages one by one until there are no more pages to load
		boolean active = true;
		while (active) {
			try {
				currentPage = PageManager.deserializePage("data/" + strTableName + "/page_" + pageNumber + ".ser");
				// page loaded successfully
				Hashtable<String, Object>[] rows = currentPage.getRows();
				
				// traversing the page row by row until a null row is encountered
				for (int i = 0; i < currentPage.getMaxRows(); i++) {
					Hashtable<String, Object> row = rows[i];
					if (row == null)
						break;
					boolean match = true;
					boolean quit = false; // quits for loop in case a PK match is found
					// checking for PK match
					if (htblColNameValue.get(primaryKey) != null
							&& htblColNameValue.get(primaryKey).equals(row.get(primaryKey))
							&& !(boolean) row.get("isDeleted")) {
						// current row matches a given primary key value, no more rows can match
						active = false;
						quit = true;
					}
					// checking current row against the given tuple
					for (String tableKey : tableKeys) {
						if (htblColNameValue.get(tableKey) != null
								&& !htblColNameValue.get(tableKey).equals(row.get(tableKey))) {
							match = false;
							break;
						}
					}

					if (match && !(boolean) row.get("isDeleted")) {
						// current row matches the given tuple, proceed in deleting it
						row.put("isDeleted", true);
						deleteCount++;
					}

					if (quit)
						break;
				}

				PageManager.serializePage(currentPage,
						"data/" + strTableName + "/page_" + currentPage.getPageNumber() + ".ser");
				pageNumber++;

			} catch (FileNotFoundException e) {
				// there are no more pages in this table
				active = false; // break out of the while loop to finalize
			} catch (Exception e) {
				// unexpected exception
				e.printStackTrace();
			}
		}
		// finalization
		System.out.printf("%d row(s) deleted\n", deleteCount);
	}
}
