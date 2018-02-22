package team10;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Hashtable;

public class UpdateUtilities {

	public static boolean checkNotAllNulls(Hashtable<String, Object> htblColNameValue) {
		int n = htblColNameValue.size();
		for (String key : htblColNameValue.keySet())
			if (htblColNameValue.get(key) == null)
				n--;
		return (n == 0);

	}

	// Here I will be returning an Arraylist of two things: the Hashtable, and
	// the Primary Key column name
	public static ArrayList<Object> getColumnsAndKey(String strTableName) {
		String line = null;
		BufferedReader br = null;
		try {
			br = new BufferedReader(new FileReader("data/metadata.csv"));
			line = br.readLine();
		} catch (IOException e) {
			e.printStackTrace();
		}

		String PKey = null;

		Hashtable<String, String> ColNameType = new Hashtable<>();

		while (line != null) {
			String[] content = line.split(",");

			if (content[0].equals(strTableName)) {
				ColNameType.put(content[1], content[2]);
				if ((content[3].toLowerCase()).equals("true"))
					PKey = content[1];
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

		ArrayList<Object> Data = new ArrayList<>();
		Data.add(ColNameType);
		Data.add(PKey);

		return Data;
	}

	public static boolean checkNotUsed(String tableName, Object newValue, String PKName) {
		int pageNum = 1;
		while (true) {
			try {
				Page curPage = PageManager.deserializePage("data/" + tableName + "/" + "page_" + pageNum + ".ser");
				for (int i = 0; i < curPage.getRows().length; i++) {
					Hashtable<String, Object> curRow = curPage.getRows()[i];
					// if a matching row is found
					if (curRow.get(PKName).equals(newValue) && !((boolean) curRow.get("isDeleted"))) {
						return false;
					}
				}
			} catch (Exception e) {
				return true;
			}
		}
	}

}
