package cn.edu.thu.thss.log;

import java.net.InetAddress;

import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessageOut;
import org.apache.cassandra.net.MessagingService.Verb;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class StalenessLogger {
	private static final Logger logger = LoggerFactory.getLogger(StalenessLogger.class);
	private static final String TEST_KEYSPACE = "test_metrics";
	
	public static final String CDR_NODE_ENQUEUE = "coordinatorNodeEnqueue";
	public static final String CDR_NODE_SEND = "coordinatorNodeSend";
	public static final String SUB_NODE_RECEIVE = "subNodeReceive";
	public static final String SUB_NODE_ENQUEUE = "subNodeEnqueue";
	public static final String SUB_NODE_APPLY = "subNodeApply";
	public static final String SUB_NODE_FINISH = "subNodeFinish";
	public static final String CDR_NODE_APPLY_ENQUEUE = "coordinatorNodeApplyEnqueue";
	public static final String CDR_NODE_APPLY_START = "coordinatorNodeApplyStart";
	public static final String CDR_NODE_APPLY_FINISH = "coordinatorNodeApply";
	
	public static void messageOutToLog(MessageOut mo, String mid, long timeStamp, InetAddress dest, String timeType) {
		if (mo.verb == Verb.MUTATION && 
			mo.payload instanceof RowMutation && 
			((RowMutation)mo.payload).getTable().startsWith(TEST_KEYSPACE)) {
			RowMutation rm = (RowMutation)mo.payload;
			StringBuffer sb = new StringBuffer();
			sb.append(rm.getRowMutationId() + "\t");
			sb.append(mid + "\t");
			sb.append(dest.getHostAddress() + "\t");
			sb.append(timeType + "\t");
			sb.append(timeStamp + "\t");
			sb.append(rm.getTable() + "\t");
			sb.append(ByteBufferUtil.bytesToHex(rm.key()));
			logger.info(sb.toString());
		}
	}
	
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
	
}
