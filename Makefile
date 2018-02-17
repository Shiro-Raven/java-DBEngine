all: src/placeholder/DBApp.java src/placeholder/InsertionUtilities.java src/placeholder/Page.java src/placeholder/PageManager.java src/placeholder/Tests.java src/placeholder/DBAppException.java
	javac src/placeholder/DBApp.java src/placeholder/Page.java src/placeholder/PageManager.java src/placeholder/InsertionUtilities.java src/placeholder/Tests.java src/placeholder/DBAppException.java
	mv src/placeholder/*.class classes/placeholder/
	java -cp classes/ placeholder.Tests

clean:
	rm classes/placeholder/*.class