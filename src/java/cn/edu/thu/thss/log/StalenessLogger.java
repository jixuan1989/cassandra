package cn.edu.thu.thss.log;

import java.net.InetAddress;

import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService.Verb;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 记录写操作流程中时间点的日志类
 * @author EricPai
 */
public class StalenessLogger {
	private static final Logger logger = LoggerFactory.getLogger(StalenessLogger.class);
	private static final String TEST_KEYSPACE = "test_metrics";//要记录日志的KS的前缀
	
	public static final String CDR_NODE_ENQUEUE = "coordinatorNodeEnqueue";
	public static final String CDR_NODE_SEND = "coordinatorNodeSend";
	public static final String SUB_NODE_RECEIVE = "subNodeReceive";
	public static final String SUB_NODE_ENQUEUE = "subNodeEnqueue";
	public static final String SUB_NODE_APPLY = "subNodeApply";
	public static final String SUB_NODE_FINISH = "subNodeFinish";
	public static final String CDR_NODE_APPLY_ENQUEUE = "coordinatorNodeApplyEnqueue";
	public static final String CDR_NODE_APPLY_START = "coordinatorNodeApplyStart";
	public static final String CDR_NODE_APPLY_FINISH = "coordinatorNodeApplyFinish";
	
	/**
	 * 记录当本节点为协调者节点时MessageOut的重要时间点
	 * @param mo MessageOut对象
	 * @param mid MessageOut的Id
	 * @param timeStamp 时间点
	 * @param dest 目的IP地址
	 * @param timeType 时间点类型，包括入队时间<code>StalenessLogger.<i>CDR_NODE_ENQUEUE</i></code>
	 * 			和发送完成时间<code>StalenessLogger.<i>CDR_NODE_SEND</i></code>
	 */
	public static void messageOutToLog(MessageOut mo, String mid, long timeStamp, InetAddress dest, String timeType) {
		if (mo.verb == Verb.MUTATION && 
			mo.payload instanceof RowMutation && 
			((RowMutation)mo.payload).getTable().startsWith(TEST_KEYSPACE)) {
			RowMutation rm = (RowMutation)mo.payload;
			StringBuffer sb = new StringBuffer();
			sb.append(rm.getRowMutationId() + "\t");
			sb.append(mid + "\t");
			sb.append(dest == null ? "null" : dest.getHostAddress());
			sb.append('\t');
			sb.append(timeType + "\t");
			sb.append(timeStamp + "\t");
			sb.append(rm.getTable() + "\t");
			sb.append(ByteBufferUtil.bytesToHex(rm.key()));
			logger.info(sb.toString());
		}
	}
	
	/**
	 * 记录当本节点为非协调者节点时MessageIn的重要时间点
	 * @param mi MessageIn对象
	 * @param mid MessageIn的Id
	 * @param timeStamp 时间点
	 * @param timeType 时间点类型，包括开始接收时间<code>StalenessLogger.<i>SUB_NODE_RECEIVE</i></code>,
	 * 			处理队列入队开始时间<code>StalenessLogger.<i>SUB_NODE_ENQUEUE</i></code>,
	 * 			处理开始时间<code>StalenessLogger.<i>SUB_NODE_APPLY</i></code>,
	 * 			和处理完成时间<code>StalenessLogger.<i>SUB_NODE_FINISH</i></code>
	 */
	public static void messageInToLog(MessageIn mi, String mid, long timeStamp, String timeType) {
		if (mi.verb == Verb.MUTATION && 
			mi.payload instanceof RowMutation && 
			((RowMutation)mi.payload).getTable().startsWith(TEST_KEYSPACE)) {
			RowMutation rm = (RowMutation)mi.payload;
			StringBuffer sb = new StringBuffer();
			sb.append(rm.getRowMutationId() + "\t");
			sb.append(mid + "\t");
			sb.append(mi.from.getHostAddress() + "\t");
			sb.append(timeType + "\t");
			sb.append(timeStamp + "\t");
			sb.append(rm.getTable() + "\t");
			sb.append(ByteBufferUtil.bytesToHex(rm.key()));
			logger.info(sb.toString());
		}
	}
	
	/**
	 * 记录当本节点为协调者节点时本地写操作执行的重要时间点
	 * @param rm RowMutation对象
	 * @param timeStamp 时间点
	 * @param localAddr 本地IP地址
	 * @param timeType 时间点类型，包括处理队列入队开始时间<code>StalenessLogger.<i>CDR_NODE_APPLY_ENQUEUE</i></code>,
	 * 			处理开始时间<code>StalenessLogger.<i>CDR_NODE_APPLY_START</i></code>,
	 * 			和处理完成时间<code>StalenessLogger.<i>CDR_NODE_APPLY_FINISH</i></code>
	 */
	public static void coordinatorLocalApplyToLog(RowMutation rm, long timeStamp, InetAddress localAddr, String timeType) {
		if (rm.getTable().startsWith(TEST_KEYSPACE)) {
				StringBuffer sb = new StringBuffer();
				sb.append(rm.getRowMutationId() + "\t");
				sb.append("0\t");
				sb.append(localAddr.getHostAddress() + "\t");
				sb.append(timeType + "\t");
				sb.append(timeStamp + "\t");
				sb.append(rm.getTable() + "\t");
				sb.append(ByteBufferUtil.bytesToHex(rm.key()));
				logger.info(sb.toString());
		}
	}
	
	/**
	 * 得到当前的时间点
	 * @return long类型表示的时间点,目前为纳秒级别
	 */
	public static long getCurrentTime() {
		return System.nanoTime();
	}
	
}
