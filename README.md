# Simple Spark exercise

1. Read a csv file from local filesystem.
2. Remove rows where any string column is a empty string or contains only spaces.
    *Note* : empty string is not same as null
3. Convert columns' data types and names according to the user’s choice
4. For a given column, provide profiling information:
    * total number of unique values,
    * count of each unique value,
    * exclude - nulls.
    
# Configuration
Configuration is read from config.yaml located in _resources_ folder.
The following attributes are configurable.
* sparkAppName
* sparkMaster
* dataModel - data file configuration (columns names and data types, see below)
* dataFile- data source csv file
* dataHeaderExists - true: datafile contains header; false: datafile does not contain header 
* dataDelimiter - data file delimiter
* dataQuote - columns enclosing symbol
* dataDateFormat
* dataTimestampFormat
* reportFile - final report name

# Building
``` $ ./gradlew build ```
# Running
``` $ spark-submit build/libs/spark-scala-ex01.jar ```