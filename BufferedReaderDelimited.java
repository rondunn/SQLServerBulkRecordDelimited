
package SQLServerBulkRecordDelimited;

import java.io.Reader;
import java.io.IOException;

public class BufferedReaderDelimited {
	
	// Input stream reader
    protected Reader reader;
    
    // Line delimiter
    protected char[] lineDelimiter;
    protected int lineDelimiterLength;

	// Buffer to hold data from reader
	protected char[] buf;
    
    // Default buf size, updated by constructor
	protected static int BUFSIZE = 4096;
   
	// Number of characters in buffer
    protected int bufLength;
    
    // Index current position in buffer
    protected int bufIndex = 0;

	// Default output line size
    protected static int LINESIZE = 1024;

	/**
	* Construct DelimitedReader with user-defined buffer size
	*
	* @param  reader   Stream reader to buffer
	* @param  bufsize  Stream buffer Size, default 4096
	*/
	
	public BufferedReaderDelimited(Reader reader, int bufsize) {
		if (bufsize < 1024) throw new IllegalArgumentException("bufsize < 1024");
		this.reader = reader;
		buf = new char[bufsize];
		bufIndex = 0;
		}

 	/**
	* Construct DelimitedReader with default size 4096
	*
	* @param  reader  Stream reader to buffer
	*/
	
    public BufferedReaderDelimited(Reader reader) {
        this (reader,BUFSIZE);
    	}
    	
    /**
    * Close BufferedReaderDelimited
    */

    public void close() {
        if (reader == null) return;
        reader = null;
        buf = null;
    	}
    	
	/**
	* Fill input buffer from reader
	 * @throws java.io.IOException
	*/
	
    protected void fill() throws IOException {
		bufLength = reader.read(buf, 0, buf.length);
 		bufIndex = 0;
		}
		
	/**
	* Read line
	*
	* @return String line, or null on end of reader.
	 * @throws java.io.IOException
	*/

     public String readLine() throws IOException {
    	
		StringBuffer s = null;
		int startIndex = bufIndex;

		while (1==1) {

			// Fill the buffer if we've passed the end.
			if (bufIndex >= bufLength) {
				fill();
				startIndex = 0;
				}
			
			// If we're still at the end, we've reached the end of file.
			if (bufIndex >= bufLength) {
				if (s != null && s.length() > 0) return s.toString();
				else return null;
				}
				
			// Step through the buffer until we find a delimiter or reach the buffer end.
			boolean eol = false;
			while (bufIndex < bufLength) {
				
				// Look for the delimiter sequence
				boolean gotDelimiter = true;
				if (lineDelimiterLength == 1) {
					if (buf[bufIndex] != lineDelimiter[0]) gotDelimiter = false;
					}
				else {
					for (int j=0; j<lineDelimiterLength; ++j) {
						if (buf[bufIndex+j] != lineDelimiter[j]) {
							gotDelimiter = false;
							break;
							}
						}
					}
				
				// Break if we got a valid delimiter ...	
				if (gotDelimiter) {
					eol = true;
					break;
					}
					
				// ... otherwise keep stepping through stream
				++ bufIndex;
				}
				
			// Append the buffer to the string.
			if (s == null) s = new StringBuffer (LINESIZE);
			s.append(buf, startIndex, bufIndex-startIndex);
		
			// Return a value if we're at end of line
			if (eol) {
				
				// Increment the buffer index to step past the delimiter
				// ready for our next call to this function
				bufIndex += lineDelimiter.length;
				
				// Return the string
				return s.toString();
				}
				
			}
		}
		
	/**
	* Set line delimiter from string.
	*
	* @param delimiter	Line delimiter as string (eg "\r\n").
	*/
		
	public void setDelimiter (String delimiter) {
		this.lineDelimiter = delimiter.toCharArray();
		this.lineDelimiterLength = this.lineDelimiter.length;
		}
		
	/**
	* Set line delimiter from character array, to support non-printable delimiters.
	*
	* @param delimiter	Line delimiter as character array (eg new char[] { 13, 10 }).
	*/
		
	public void setDelimiter (char[] delimiter) {
		this.lineDelimiter = delimiter;
		this.lineDelimiterLength = this.lineDelimiter.length;
		}

	}
