package team10;

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
		while (true) {
			try {
				currentPage = PageManager.deserializePage("data/" + strTableName + "/page_" + pageNumber + ".ser");
				// page loaded successfully
				Hashtable<String, Object>[] rows = currentPage.getRows();

				for (int i = 0; i < currentPage.getMaxRows(); i++) {
					Hashtable<String, Object> row = rows[i];

					if (htblColNameValue.get(primaryKey) != null) {
						// provided tuple had a primary key i.e. only a single row to delete
						if (htblColNameValue.get(primaryKey).equals(row.get(primaryKey))
								&& !(boolean) row.get("isDeleted")) {
							// row found, delete and finalize
							row.put("isDeleted", true);
							deleteCount++;
							PageManager.serializePage(currentPage,
									"data/" + strTableName + "/page_" + pageNumber + ".ser");
							System.out.printf("%d row(s) deleted\n", deleteCount);
							return;
						}
					} else {
						// provided tuple had no primary key i.e. multiple rows to delete
						boolean match = true;
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
					}
				}

				pageNumber++;

			} catch (Exception e) {
				// there are no more pages in this table
				break; // break out of the while loop to finalize
			}
		}
		// finalization
		PageManager.serializePage(currentPage, "data/" + strTableName + "/page_" + pageNumber + ".ser");
		System.out.printf("%d row(s) deleted\n", deleteCount);
	}
}
