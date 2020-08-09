package org.apache.flink.table.runtime.ml.python.mlframework.statemachine;

import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event.WorkStopEvent;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class MLMeta {
	//单例模式，保证coordinator和enumerator共享同一Meta

	private static final MLMeta ML_META = new MLMeta();

	/**
	 * work start flag
	 */
	private static boolean workStart;

	/**
	 * operator register num
	 */
	private int registerNodeNum;

	private int lastNodeNum;

	/**
	 * users config node num
	 */
	public HashMap<String, Integer> nodeNumMap = new HashMap<>();

	/**
	 * cluster info by register
	 */
	private HashMap<String, ArrayList<String>> clusterInfo = new HashMap<>();

	/**
	 * block queue when worker stop
	 */
	public BlockingQueue<WorkStopEvent> workStopEventQueue = new ArrayBlockingQueue<>(1000);

	private MLMeta() {

	}

	public static MLMeta getMlMeta(){
		return ML_META;
	}

	public static boolean isWorkStart() {
		return workStart;
	}

	public static void setWorkStart(boolean workStart) {
		MLMeta.workStart = workStart;
	}

	public int getConfigNodeNum() {
		int nodenum = 0;
		for(Map.Entry<String, Integer> entry : nodeNumMap.entrySet()) {
			nodenum += entry.getValue();
		}
		return nodenum;
	}

	public int getRegisterNodeNum() {
		return registerNodeNum;
	}

	public void setRegisterNodeNum(int registerNodeNum) {
		this.registerNodeNum = registerNodeNum;
	}

	public HashMap<String, ArrayList<String>> getClusterInfo() {
		return clusterInfo;
	}

	public void setClusterInfo(HashMap<String, ArrayList<String>> clusterInfo) {
		this.clusterInfo = clusterInfo;
	}

	public int getLastNodeNum() {
		return lastNodeNum;
	}

	public void setLastNodeNum(int lastNodeNum) {
		this.lastNodeNum = lastNodeNum;
	}
}
