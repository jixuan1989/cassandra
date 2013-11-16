package cn.edu.thu.thss.log;

import java.io.File;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.TimeZone;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

/**
 * Rolling file by users' profile
 * @author EricPai
 */
public abstract class AbstractMinRollingFileAppender extends FileAppender {
	
	private final String DATE_PATTERN = "'.'yyyy-MM-dd-HH-mm";

	/**
	 * The log file will be renamed to the value of the scheduledFilename
	 * variable when the next interval is entered. For example, if the rollover
	 * period is one hour, the log file will be renamed to the value of
	 * "scheduledFilename" at the beginning of the next hour.
	 * 
	 * The precise time when a rollover occurs depends on logging activity.
	 */
	private String scheduledFilename;

	/**
	 * The next time we estimate a rollover should occur.
	 */
	private long nextCheck = System.currentTimeMillis() - 1;
	

	Date now = new Date();

	SimpleDateFormat sdf;

	GregorianCalendar rc = new GregorianCalendar();

	// The gmtTimeZone is used only in computeCheckPeriod() method.
	static final TimeZone gmtTimeZone = TimeZone.getTimeZone("GMT");

	/**
	 * The default constructor does nothing.
	 */
	public AbstractMinRollingFileAppender() {
	}

	/**
	 * Instantiate a <code>DailyRollingFileAppender</code> and open the file
	 * designated by <code>filename</code>. The opened filename will become the
	 * ouput destination for this appender.
	 */
	public AbstractMinRollingFileAppender(Layout layout, String filename) throws IOException {
		super(layout, filename, true);
		activateOptions();
	}

	/** Returns the value of the <b>DatePattern</b> option. */
	public String getDatePattern() {
		return DATE_PATTERN;
	}

	public void activateOptions() {
		super.activateOptions();
		if (DATE_PATTERN != null && fileName != null) {
			now.setTime(System.currentTimeMillis());
			sdf = new SimpleDateFormat(DATE_PATTERN);
			File file = new File(fileName);
			scheduledFilename = fileName
					+ sdf.format(new Date(file.lastModified()));

		} else {
			LogLog.error("Either File or DatePattern options are not set for appender ["
					+ name + "].");
		}
	}

	/**
	 * Rollover the current file to a new file.
	 */
	void rollOver() throws IOException {

		/* Compute filename, but only if datePattern is specified */
		if (DATE_PATTERN == null) {
			errorHandler.error("Missing DatePattern option in rollOver().");
			return;
		}

		String datedFilename = fileName + sdf.format(now);
		// It is too early to roll over because we are still within the
		// bounds of the current interval. Rollover will occur once the
		// next interval is reached.
		if (scheduledFilename.equals(datedFilename)) {
			return;
		}

		// close current file, and rename it to datedFilename
		this.closeFile();
		
		Calendar rollTime = Calendar.getInstance();
		rollTime.set(Calendar.MINUTE, getPrevRollMinute(rollTime));
		String minuteLog = fileName + sdf.format(rollTime.getTime());
		File target = new File(minuteLog);
		
		if (target.exists()) {
			target.delete();
		}

		File file = new File(fileName);
		boolean result = file.renameTo(target);
		if (result) {
			LogLog.debug(fileName + " -> " + scheduledFilename);
		} else {
			LogLog.error("Failed to rename [" + fileName + "] to ["
					+ scheduledFilename + "].");
		}

		try {
			// This will also close the file. This is OK since multiple
			// close operations are safe.
			this.setFile(fileName, true, this.bufferedIO, this.bufferSize);
		} catch (IOException e) {
			errorHandler.error("setFile(" + fileName + ", true) call failed.");
		}
		scheduledFilename = datedFilename;
	}

	/**
	 * This method differentiates DailyRollingFileAppender from its super class.
	 * 
	 * <p>
	 * Before actually logging, this method will check whether it is time to do
	 * a rollover. If it is, it will schedule the next rollover time and then
	 * rollover.
	 * */
	protected void subAppend(LoggingEvent event) {
		long n = System.currentTimeMillis();
		if (n >= nextCheck) {
			now.setTime(n);
			rc.setTime(now);
			nextCheck = getNextCheckMillis(rc);
			try {
				rollOver();
			} catch (IOException ioe) {
				if (ioe instanceof InterruptedIOException) {
					Thread.currentThread().interrupt();
				}
				LogLog.error("rollOver() failed.", ioe);
			}
		}
		String obj = this.layout.format(event);
		if (obj != null && !obj.equals("") && !obj.equals(" /r/n") && !obj.equals(" /n")) {
			super.subAppend(event);
		}
	}
	
	/**
	 * Return the previous rolling time point in minute
	 * @param cl The relative Calendar
	 * @return The previous rolling time point in minute
	 */
	abstract int getPrevRollMinute(Calendar cl);
	
	/**
	 * Return the next rolling time point
	 * @param cl The relative Calendar
	 * @return The next rolling time point
	 */
	abstract long getNextCheckMillis(Calendar cl);
}
