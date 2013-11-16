package cn.edu.thu.thss.log;

import java.util.Calendar;

/**
 * 每个小时的0分，20分，40分各归档一次日志，参考了DailyRollingFileAppender
 * @author EricPai
 */
public class TwentyMinRollingFileAppender extends AbstractMinRollingFileAppender {
	
	/**
	 * Return the previous rolling time point
	 * @param cl The relative Calendar
	 * @return <b>0</b> if current minute is in <b>[0, 20)</b>, 
	 * 			<b>20</b> if <b>[20, 40)</b>, 
	 * 			and <b>40</b> if <b>[40, 59]</b>
	 */
	@Override
	protected int getPrevRollMinute(Calendar cl) {
		int minute = cl.get(Calendar.MINUTE);
		int rollMin = 20;
		if (minute < 20) {
			rollMin = 0;
		} else if (minute >= 40){
			rollMin = 40;
		}
		return rollMin;
	}

	@Override
	protected long getNextCheckMillis(Calendar cl) {
		cl.set(Calendar.SECOND, 0);
		cl.set(Calendar.MILLISECOND, 0);
		int minute = cl.get(Calendar.MINUTE);
		if (minute < 20) {
			cl.set(Calendar.MINUTE, 20);
		} else if (minute < 40 && minute >= 20){
			cl.set(Calendar.MINUTE, 40);
		} else {
			cl.set(Calendar.MINUTE, 0);
			cl.add(Calendar.HOUR_OF_DAY, 1);
		}
		return cl.getTime().getTime();
	}
}
