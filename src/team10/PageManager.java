package team10;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.PrintWriter;
import java.util.Properties;

public class PageManager {
	public static void serializePage(Page page, String filepath) throws IOException {
		FileOutputStream fileOut = new FileOutputStream(filepath);
		ObjectOutputStream out = new ObjectOutputStream(fileOut);
		out.writeObject(page);
		out.close();
		fileOut.close();

		// Testing Purposes // To Be Removed //
		PrintWriter writer = new PrintWriter(filepath.substring(0, filepath.length() - 3) + "txt");
		writer.print(page);
		writer.close();
	}

	public static Page deserializePage(String filepath) throws IOException, ClassNotFoundException {
		FileInputStream fileIn = new FileInputStream(filepath);
		ObjectInputStream in = new ObjectInputStream(fileIn);
		Page page = (Page) in.readObject();
		in.close();
		fileIn.close();
		return page;
	}

	public static int getMaximumRowsCountinPage() throws IOException {
		FileReader fileReader = new FileReader("config/DBApp.properties");
		Properties p = new Properties();
		p.load(fileReader);
		return Integer.parseInt(p.getProperty("MaximumRowsCountinPage"));
	}

	public static void printPageContents(Page page) {
		for (int i = 0; i < page.getMaxRows(); i++) {
			if (page.getRows()[i] == null) {
				break;
			} else {
				System.out.println("Row " + i + ": " + page.getRows()[i].toString());
			}
		}

		System.out.println("End of Page " + page.getPageNumber());
	}

	public static int getBRINSize() throws IOException {
		FileReader fileReader = new FileReader("config/DBApp.properties");
		Properties p = new Properties();
		p.load(fileReader);
		return Integer.parseInt(p.getProperty("BRINSize"));
	}
	
	static Page loadPageIfExists(String filepath) {

		try {

			return PageManager.deserializePage(filepath);

		} catch (Exception e) {

			return null;

		}

	}

}
