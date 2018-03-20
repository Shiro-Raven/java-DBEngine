all: src/team10/CreationUtilities.java src/team10/DBApp.java src/team10/DBAppException.java src/team10/DBAppTest.java src/team10/DeletionUtilities.java src/team10/IndexUtilities.java src/team10/InsertionUtilities.java src/team10/Page.java src/team10/PageManager.java src/team10/PageType.java src/team10/RandomString.java src/team10/SelectionUtilities.java src/team10/Tests.java src/team10/UpdateUtilities.java
	javac src/team10/CreationUtilities.java src/team10/DBApp.java src/team10/DBAppException.java src/team10/DBAppTest.java src/team10/DeletionUtilities.java src/team10/IndexUtilities.java src/team10/InsertionUtilities.java src/team10/Page.java src/team10/PageManager.java src/team10/PageType.java src/team10/RandomString.java src/team10/SelectionUtilities.java src/team10/Tests.java src/team10/UpdateUtilities.java
	mv src/team10/*.class classes/team10/
	java -cp classes/ team10.DBAppTest

clean:
	rm classes/team10/*.class
	rm -rf data
	mkdir data
	touch data/metadata.csv