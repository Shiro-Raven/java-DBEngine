all: src/team10/DBApp.java src/team10/InsertionUtilities.java src/team10/Page.java src/team10/PageManager.java src/team10/Tests.java src/team10/DBAppException.java src/team10/CreationUtilities.java
	javac src/team10/DBApp.java src/team10/Page.java src/team10/PageManager.java src/team10/InsertionUtilities.java src/team10/Tests.java src/team10/DBAppException.java src/team10/CreationUtilities.java
	mv src/team10/*.class classes/team10/
	java -cp classes/ team10.Tests

clean:
	rm classes/team10/*.class