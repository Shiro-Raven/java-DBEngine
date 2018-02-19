package team10;

import java.util.Date;
import java.util.Hashtable;

public class Tests {

	@SuppressWarnings("deprecation")
	public static void main(String[] args) throws Exception {
		
		Hashtable<String, Object> htblColNameValue = new Hashtable<>();
		htblColNameValue.put("id", 392);
		htblColNameValue.put("first_name", "Ahmed");
		htblColNameValue.put("last_name", "Mohammed");
		htblColNameValue.put("gpa", 1.91);
		htblColNameValue.put("birth_date", new Date(1997, 5, 5));
		htblColNameValue.put("gender", false);
		new DBApp().insertIntoTable("Student", htblColNameValue);
		
		Page page = PageManager.deserializePage("data/Student/page_1.ser");
		PageManager.printPageContents(page);

	}

}