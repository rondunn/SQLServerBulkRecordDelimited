
package SQLServerBulkRecordDelimited;

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.sql.Types;
import com.microsoft.sqlserver.jdbc.*;
import java.text.SimpleDateFormat;
import java.time.ZoneId;

public class SQLServerBulkRecordDelimited implements ISQLServerBulkRecord, java.lang.AutoCloseable {
    
	//--------------------------------------------------------------------------
	// Column metadata
	//--------------------------------------------------------------------------
	
	protected class Column {
        String name;
        Integer type;
        Integer precision;
        Integer scale;
        SimpleDateFormat format = null;

        Column(String name,Integer type,Integer precision,Integer scale,SimpleDateFormat format) {
            this.name = name;
            this.type = type;
            this.precision = precision;
            this.scale = scale;
            this.format = format;
			}
		}
	
	protected List<Column> columns = new ArrayList<>();

	//--------------------------------------------------------------------------
	//	File properties
	//--------------------------------------------------------------------------
	
	protected String fileName = null;
	protected Integer skipLines = 0;
	protected char[] rowDelimiter = "\n".toCharArray();
	protected Integer rowDelimiterLength = 1;
	protected String colDelimiter = ",";
	protected String encoding = "UTF-8";
	protected String nullText = null;
	
	// Default format strings for dates and times.
	protected String defaultFormatDate = "y-M-d";
	protected String defaultFormatTime = "H:m:s";
	protected String defaultFormatTimestamp = "y-M-d H:m:s";
	protected String defaultFormatTimestampWithTimezone = "y-M-d H:m:s X";
	
	// File interface
    protected BufferedReaderDelimited reader;
	protected int readerBufferSize = 4096;
    protected InputStreamReader isr;
    protected FileInputStream fis;
	
	// Row retrieved from file
	protected String row;
	protected Integer rowCount = 0;
	
	//--------------------------------------------------------------------------
	//	Fluent interface to set properties
	//--------------------------------------------------------------------------
	
	/**
	 * Set the BufferedReaderDelimited buffer size. Default is 4096.
	 * @param bufferSize
	 * @return this
	 */
	
	public SQLServerBulkRecordDelimited bufferSize (int bufferSize) {
		this.readerBufferSize = bufferSize;
		return this;
		}
	
	/**
	 * Specify the column delimiter to be used when parsing an input line.
	 * @param delimiter String containing one or more characters
	 * @return this
	 */
	
	public SQLServerBulkRecordDelimited colDelimiter (String delimiter) {
		this.colDelimiter = delimiter;
		return this;
		}
	
	/**
	 * Add a column definition to the load metadata. All columns in the file
	 * must be defined in order.
	 * @param name			Name of column in target database
	 * @param type			java.sql.Types data type
	 * @param precision		Precision / length
	 * @param scale			Decimal places
	 * @param format		Format string for dates, times, etc.
	 * @return	this		Designed to support fluent API
	 * @throws Exception 
	 */
	
	public SQLServerBulkRecordDelimited column (String name,int type,int precision,int scale,String format) throws Exception {
		
		if (name == null) throw new Exception ("name = null");
		for (Column col: columns) if (col.name.equalsIgnoreCase(name)) throw new Exception (name + " already defined");
		SimpleDateFormat sdf = null;
		
		// Override requested column properties if required by data type.
        switch (type) {
			
			// Allocate buffer and default format for dates and times.
            case java.sql.Types.DATE:
            case java.sql.Types.TIME:
            case java.sql.Types.TIMESTAMP:
			case java.sql.Types.TIMESTAMP_WITH_TIMEZONE:

				precision = 32;
				
				if (type == Types.DATE && format == null) format = defaultFormatDate;
				else if (type == Types.TIME && format == null) format = defaultFormatTime;
				else if (type == Types.TIMESTAMP && format == null) format = defaultFormatTimestamp;
				else if (type == Types.TIMESTAMP_WITH_TIMEZONE && format == null) format = defaultFormatTimestampWithTimezone;
				if (format != null) sdf = new SimpleDateFormat (format);

				break;

            // Redirect SQLXML as LONGNVARCHAR, SQLXML is not valid type in TDS
            case java.sql.Types.SQLXML:
				type = java.sql.Types.LONGNVARCHAR;
                break;

            // Redirect Float as Double based on data type mapping
            case java.sql.Types.FLOAT:
				type = java.sql.Types.DOUBLE;
                break;

            // Redirect BOOLEAN as BIT
            case java.sql.Types.BOOLEAN:
				type = java.sql.Types.BIT;
                break;
				
			}	
		
		Column col = new Column (name,type,precision,scale,sdf);
		columns.add (col);
		
		return this;
		}
	
	/**
	 * Define a column with just name and type. Useful for data types that
	 * don't need precision and scale, such as integers and bits.
	 * @param name	Column name
	 * @param type	java.sql.Types data type
	 * @return this
	 * @throws Exception 
	 */
	
	public SQLServerBulkRecordDelimited column (String name,int type) throws Exception {
		return column (name,type,0,0,null);
		}
	
	/**
	 * Define a column with name, type and precision. Useful for varchar-types
	 * that need a length.
	 * @param name	Column name
	 * @param type	java.sql.Types data type
	 * @param precision Column length
	 * @return this
	 * @throws Exception 
	 */
	
	public SQLServerBulkRecordDelimited column (String name,int type,int precision) throws Exception {
		return column (name,type,0,0,null);
		}
	
	/**
	 * Define a column with name, type and format. Useful for temporal-types
	 * that take a format string.
	 * @param name	Column name
	 * @param type	java.sql.Types data type
	 * @param format DateTimeFormatter format string
	 * @return this
	 * @throws Exception 
	 */
	
	public SQLServerBulkRecordDelimited column (String name,int type,String format) throws Exception {
		return column (name,type,0,0,format);
		}
	
	/**
	 * Set the encoding of the input file. Defaults to UTF-8.
	 * @param encoding
	 * @return this
	 */

	public SQLServerBulkRecordDelimited encoding (String encoding) {
		this.encoding = encoding;
		return this;
		}
	
	/**
	 * Set path name of file to be loaded.
	 * @param fileName
	 * @return this
	 */

	public SQLServerBulkRecordDelimited fileName (String fileName) {
		this.fileName = fileName;
		return this;
		}
	
	/**
	 * Set the format of Dates. Default is y-M-d. May be overridden by the
	 * column definition. Target database type should be DATE.
	 * @param format	SimpleDateFormat format string.
	 * @return this
	 */
	
	public SQLServerBulkRecordDelimited formatDate(String format) {
		this.defaultFormatDate = format;
		return this;
		}
	
	/**
	 * Set the format of Times. Default is H:m:s. May be overridden by the
	 * column definition. Target database type should be TIME.
	 * @param format	SimpleDateFormat format string.
	 * @return this
	 */
	
	public SQLServerBulkRecordDelimited formatTime(String format) {
		this.defaultFormatDate = format;
		return this;
		}
	
	/**
	 * Set the format of Timestamps. Default is y-M-d H:m:s. May be
	 * overridden by the column definition. Target database type should be 
	 * DATETIME or DATETIME2.
	 * @param format	SimpleDateFormat format string
	 * @return this
	 */
	
	public SQLServerBulkRecordDelimited formatTimestamp (String format) {
		this.defaultFormatTimestamp = format;
		return this;
		}
	
	/**
	 * Set the format of Timestamps with Timezones. Default is y-M-d H:m:s X. May be
	 * overridden by the column definition. Target database type should be 
	 * DATETIMEOFFSET, and values will be stored in GMT.
	 * @param format	SimpleDateFormat format string
	 * @return this
	 */
	
	public SQLServerBulkRecordDelimited formatTimestampWithTimezone (String format) {
		this.defaultFormatTimestamp = format;
		return this;
		}
	
	/**
	 * Set the value that will differentiate NULL from an empty string
	 * in character fields. If this property is not specified, any empty
	 * value will be loaded as NULL.
	 * @param text NULL column value, defaults to NULL
	 * @return this
	 */
	
	public SQLServerBulkRecordDelimited nullText (String text) {
		this.nullText = text;
		return this;
		}
	
	/**
	 * Open the file for loading.
	 * @return this
	 * @throws SQLServerException 
	 */
	
	public SQLServerBulkRecordDelimited open() throws SQLServerException {
		
		try {
			// Open the input file as a reader
			this.fis = new FileInputStream (fileName);
			this.isr = new InputStreamReader (fis,encoding);
			this.reader = new BufferedReaderDelimited (isr,this.readerBufferSize);
			this.reader.setDelimiter (this.rowDelimiter);
			}
		catch (Exception ex) {
			throw new SQLServerException (ex.getMessage(),null,0,ex);
			}

		// Skip header lines
		for (int i=0; i<this.skipLines; ++i) next();
		
		return this;
		}

	/**
	 * Specify the row delimiter to be used when parsing the input file.
	 * @param delimiter String containing one or more characters
	 * @return this
	 */
	
	public SQLServerBulkRecordDelimited rowDelimiter (String delimiter) {
		this.rowDelimiter = delimiter.toCharArray();
		this.rowDelimiterLength = rowDelimiter.length;
		return this;
		}
	
	/**
	 * Specify the row delimiter to be used when parsing the input file.
	 * @param delimiter Array of one or more characters
	 * @return this
	 */
	
	public SQLServerBulkRecordDelimited rowDelimiter (char[] delimiter) {
		this.rowDelimiter = delimiter;
		this.rowDelimiterLength = rowDelimiter.length;
		return this;
		}
	
	/**
	 * Set the number of lines to skip at the beginning of a file. Used to
	 * bypass column headers.
	 * @param skipLines 
	 * @return this
	 */

	public SQLServerBulkRecordDelimited skipLines (Integer skipLines) {
		this.skipLines = skipLines;
		return this;
		}
	
	//--------------------------------------------------------------------------
	//	ISQLServerBulkRecord Interface Implementation
	//--------------------------------------------------------------------------
	
    /**
     * Release any resources associated with the file reader.
     * @throws SQLServerException
     */
	 
	@Override
    public void close() throws SQLServerException {
 
        // Ignore errors since we are only cleaning up here
        if (reader != null)
            try {
                reader.close();
				reader = null;
            	}
            catch (Exception e) {
            	}
        if (isr != null)
            try {
                isr.close();
				isr = null;
            	}
            catch (Exception e) {
            	}
        if (fis != null)
            try {
                fis.close();
				fis = null;
            	}
            catch (Exception e) {
            	}

     	}
	
	/**
	 * Get the set of column identifiers.
	 * @return set	Set of integer identifiers.
	 */

    @Override
    public Set<Integer> getColumnOrdinals() {
		Set<Integer> set = new HashSet<>();
		for (int i=0; i<columns.size(); ++i) set.add (i+1);
        return set;
		}
	
	/**
	 * Get the name of a column.
	 * @param column Base-1 number of column
	 * @return 
	 */

    @Override
    public String getColumnName(int column) {
        return columns.get(column-1).name;
		}
	
	/**
	 * Get the data type for a column
	 * @param column Base-1 number of column
	 * @return java.sql.Type value
	 */

    @Override
    public int getColumnType(int column) {
        return columns.get(column-1).type;
		}
	
	/**
	 * Get column precision.
	 * @param column
	 * @return 
	 */

    @Override
    public int getPrecision(int column) {
        return columns.get(column-1).precision;
		}
	
	/**
	 * Get column scale.
	 * @param column
	 * @return 
	 */

    @Override
    public int getScale(int column) {
       return columns.get(column-1).scale;
		}
	
	/**
	 * Auto-Increment values are not supported for delimited files.
	 * @param column IGNORE
	 * @return IGNORE
	 */

    @Override
    public boolean isAutoIncrement(int column) {
        return false;
		}
	
	/**
	 * Get array of objects representing data sent to SQL Server in specified type.
	 * @return Array of objects
	 * @throws SQLServerException 
	 */

    @Override
    public Object[] getRowData() throws SQLServerException {
        
		if (row == null) return null;

		// Split the row into value strings
		String[] values = this.row.split(colDelimiter, -1);
		if (values.length != columns.size()) {
			String msg = MessageFormat.format("Row={0}, Err={1} values, {2} expected.",rowCount,values.length,columns.size());
			throw new SQLServerException (msg,null,0,null);
			}

		// Create an array to fill with values of appropriate data type.
		Object[] o = new Object[values.length];

		// Step through the value array to create output objects
		for (int i=0; i<columns.size(); ++i) {

			Column col = columns.get(i);
			SimpleDateFormat sdf = col.format;
			String value = values[i];
			
			try {

				// Support the use of nullText to allow differentiation
				// between NULL and EMPTY strings.

				if (col.type == Types.CHAR
					|| col.type == Types.LONGNVARCHAR
					|| col.type == Types.LONGVARCHAR
					|| col.type == Types.NCHAR
					|| col.type == Types.NVARCHAR
					|| col.type == Types.VARCHAR) {
					if (this.nullText != null && value.equals(this.nullText)) value = null;
					}
				else {
					if (value.length() < 1) value = null;
					}
				
				// Perform type conversions to create output objects.

				if (value == null) o[i] = null;
				
				else if (col.type == Types.INTEGER) {
					o[i] = Integer.valueOf(value);
					}

				else if (col.type == Types.TINYINT
					|| col.type == Types.SMALLINT) {
					o[i] = Short.valueOf(value);
					}

				else if (col.type == Types.BIGINT) {
					BigDecimal bd = new BigDecimal(value.trim());
					o[i] = bd.setScale(0, RoundingMode.DOWN).longValueExact();
					}

				else if (col.type == Types.DECIMAL
					|| col.type == Types.NUMERIC) {
					BigDecimal bd = new BigDecimal(value.trim());
					o[i] = bd.setScale(col.scale, RoundingMode.HALF_UP);
					}

				else if (col.type == Types.BIT) {
					if (value.equals("0")) o[i] = 0;
					else if (value.equals("1")) o[i] = 1;
					else throw new Exception ("Binary not 1 or 0");
					}

				else if (col.type == Types.REAL) {
					o[i] = Float.parseFloat(values[i]);
					}

				else if (col.type == Types.DOUBLE) {
					o[i] = Double.parseDouble(value);
					}
				
				else if (col.type == Types.DATE) {
					o[i] = new java.sql.Date(sdf.parse (value).getTime());
					}

				else if (col.type == Types.TIME) {
					o[i] = new java.sql.Time(sdf.parse (value).getTime());
					}

				else if (col.type == Types.TIMESTAMP) {
					o[i] = new java.sql.Timestamp(sdf.parse (value).getTime());
					}

				else if (col.type == Types.TIMESTAMP_WITH_TIMEZONE) {
					o[i] = sdf.parse (value).toInstant().atZone(ZoneId.of("UTC")).toOffsetDateTime();
					}

				else if (col.type == Types.BINARY
					|| col.type == Types.VARBINARY
					|| col.type == Types.LONGVARBINARY
					|| col.type == Types.BLOB) {
					String binData = value.trim();
					if (binData.startsWith("0x") || binData.startsWith("0X")) o[i] = binData.substring(2);
					else o[i] = binData;
					}

				else if (col.type == Types.NULL) {
					o[i] = null;
					}

				else { 
					o[i] = value;
					}

				}
			
			catch (Exception e) {
				String msg = MessageFormat.format ("Row={0}, Col={1}, Val={2}, Err={3}",rowCount,i,value,e.getMessage());
				throw new SQLServerException (msg,null,0,e);
				}
			
			}
		
		// Return the object array
		return o;
		}

	/**
	 * Get next row from file.
	 * @return true on row, false at end of file
	 * @throws SQLServerException 
	 */

	@Override
	public boolean next() throws SQLServerException {
		++ this.rowCount;
		try {
			this.row = reader.readLine();
			}
		catch (Exception e) {
			String msg = MessageFormat.format ("Row={0}, Err={1}",rowCount,e.getMessage());
			throw new SQLServerException (msg,null,0,e);
			}
		return (null != this.row);
		}
	
	}
