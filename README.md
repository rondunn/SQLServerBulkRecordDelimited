# SQLServerBulkRecordDelimited

Implementation of ISQLServerBulkRecord that supports the following features:

* Multi-byte row and column delimiters
* SimpleDateFormat custom column formats for Date, Time, DateTime and DateTimeOffset columns
* Fluent API for easier configuration
* Simple demonstration program

## Configuration Settings

### .fileName(String fileName)
Fully qualified path name to file to be loaded.

### .rowDelimiter(String delimiter)<br/>.rowDelimiter(char[] delimiter)

Delimiter marking end of row. Defaults to "\n" for Linux-style newlines. Windows users may prefer to set "\r\n".

### .colDelimiter(String delimiter)<br/>.colDelimiter(char[] delimiter)

Delimiter marking end of column. Defaults to ",".

### .skipLines(int skip)

Number of lines to skip at beginning of file. Used to step over column header if present. Defaults to 0.

### .encoding(String encoding)

Encoding of file to be loaded. Defaults to "UTF-8".

### .formatDate(String format)<br/>.formatTime(String format)<br/>.formatTimestamp(String format)<br/>.formatTimestampWithTimezone(String format)

Set file-level formatting of date/time columns. Format strings correspond to the SimpleDateFormat. Default values are:

* Date y-M-d
* Time H&colon;m&colon;s
* Timestamp y-M-d H:m:s
* TimestampWithTimezone y-M-d H:m:s X

Defaults may be overriden in column() settings.

### .column(String name,java.sql.Type type)<br/>.column(String name,java.sql.Type type,int precision)<br/>.column(String name,java.sql.Type type,int precision,int scale)<br/>.column(String name,java.sql.Type type,String format)

Define the properties of a column. All columns in the file must be defined, in order of appearance in the file.

### .open()
Open file for processing.
