package cn.edu.thu.thss.log;

import java.io.IOException;
import java.io.File;
import java.io.InterruptedIOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.Calendar;
import java.util.TimeZone;
import java.util.Locale;

import org.apache.log4j.FileAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.helpers.LogLog;
import org.apache.log4j.spi.LoggingEvent;

/**
 * 每个小时的0分，20分，40分各归档一次日志，参考了DailyRollingFileAppender
 * @author EricPai
 */
public class TwentyMinRollingFileAppender extends FileAppender {
	
	private String datePattern = "'.'yyyy-MM-dd-HH-mm";

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

	RollingCalendar rc = new RollingCalendar();

	// The gmtTimeZone is used only in computeCheckPeriod() method.
	static final TimeZone gmtTimeZone = TimeZone.getTimeZone("GMT");

	/**
	 * The default constructor does nothing.
	 */
	public TwentyMinRollingFileAppender() {
	}

	/**
	 * Instantiate a <code>DailyRollingFileAppender</code> and open the file
	 * designated by <code>filename</code>. The opened filename will become the
	 * ouput destination for this appender.
	 */
	public TwentyMinRollingFileAppender(Layout layout, String filename,
			String datePattern) throws IOException {
		super(layout, filename, true);
		this.datePattern = datePattern;
		activateOptions();
	}

	/** Returns the value of the <b>DatePattern</b> option. */
	public String getDatePattern() {
		return datePattern;
	}

	public void activateOptions() {
		super.activateOptions();
		if (datePattern != null && fileName != null) {
			now.setTime(System.currentTimeMillis());
			sdf = new SimpleDateFormat(datePattern);
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
		if (datePattern == null) {
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
			nextCheck = rc.getNextCheckMillis(now);
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
	 * Return the previous rolling time point
	 * @param cl The relative Calendar
	 * @return <b>0</b> if current minute is in <b>[0, 20)</b>, 
	 * 			<b>20</b> if <b>[20, 40)</b>, 
	 * 			and <b>40</b> if <b>[40, 59]</b>
	 */
	private int getPrevRollMinute(Calendar cl) {
		int minute = cl.get(Calendar.MINUTE);
		int rollMin = 20;
		if (minute < 20) {
			rollMin = 0;
		} else if (minute >= 40){
			rollMin = 40;
		}
		return rollMin;
	}
}

/**
 * RollingCalendar is a helper class to DailyRollingFileAppender. Given a
 * periodicity type and the current time, it computes the start of the next
 * interval.
 * */
class RollingCalendar extends GregorianCalendar {
	private static final long serialVersionUID = -3560331770601814177L;

	RollingCalendar() {
		super();
	}

	RollingCalendar(TimeZone tz, Locale locale) {
		super(tz, locale);
	}

	public long getNextCheckMillis(Date now) {
		return getNextCheckDate(now).getTime();
	}

	public Date getNextCheckDate(Date now) {
		this.setTime(now);
		this.set(Calendar.SECOND, 0);
		this.set(Calendar.MILLISECOND, 0);
		int minute = get(Calendar.MINUTE);
		if (minute < 20) {
			this.set(Calendar.MINUTE, 20);
		} else if (minute < 40 && minute >= 20){
			this.set(Calendar.MINUTE, 40);
		} else {
			this.set(Calendar.MINUTE, 0);
			this.add(Calendar.HOUR_OF_DAY, 1);
		}
		return getTime();
	}
}
