package team10;

import java.io.IOException;
import java.io.Serializable;
import java.util.Hashtable;

@SuppressWarnings("serial")
public class Page implements Serializable {

	private Hashtable<String, Object>[] rows;
	private int maxRows;
	private int pageNumber;

	@SuppressWarnings("unchecked")
	public Page(int pageNumber) throws IOException {
		this.pageNumber = pageNumber;
		maxRows = PageManager.getMaximumRowsCountinPage();
		rows = (Hashtable<String, Object>[]) new Hashtable<?, ?>[maxRows];
	}

	public Hashtable<String, Object>[] getRows() {
		return rows;
	}

	public int getMaxRows() {
		return maxRows;
	}

	public int getPageNumber() {
		return pageNumber;
	}

}
