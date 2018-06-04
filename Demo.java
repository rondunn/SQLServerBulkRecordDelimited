
package SQLServerBulkRecordDelimited;

import java.sql.*;
import com.microsoft.sqlserver.jdbc.*;

public class Demo {

	public static void main(String[] args) {
		
		try {
			
			// Connect to DW

			Connection dw = DriverManager.getConnection ("jdbc:sqlserver://localhost;databaseName=myDatabase;user=myUser;password=myPassword;");
	
			// Set up the Bulk Copy

			SQLServerBulkCopy bcp = new SQLServerBulkCopy (dw);
			bcp.setDestinationTableName ("dbo.loadtest");
			
			SQLServerBulkCopyOptions copyOptions = new SQLServerBulkCopyOptions();
			copyOptions.setTableLock(true);
			copyOptions.setBulkCopyTimeout(0);
			bcp.setBulkCopyOptions(copyOptions);

			// Define the bulk record format

			SQLServerBulkRecordDelimited rec = new SQLServerBulkRecordDelimited ()
				.fileName ("/Users/ron/test.txt")
				.rowDelimiter("\n")
				.colDelimiter(",")
				.nullText("*NULL*")
				.column ("c1",java.sql.Types.INTEGER)
				.column ("c2",java.sql.Types.TIMESTAMP_WITH_TIMEZONE)
				.column ("c3",java.sql.Types.VARCHAR,30)
				.open();
			
			// Execute the bulk copy

			bcp.writeToServer (rec);

			}
		
		catch (Exception ex) {
			System.out.println(ex.getMessage());
			if (ex.getCause() != null) System.err.println (ex.getCause().getMessage());
			}
		}
	}
