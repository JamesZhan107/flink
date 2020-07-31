package org.apache.flink.table.runtime.ml.python.mlframework.statemachine;

import org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event.WorkStopEvent;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class MLMeta {
	//单例模式，保证coordinator和enumerator共享同一Meta

	private static final MLMeta ML_META = new MLMeta();
	private static boolean workStart;
	private static int nodeNum;
	private static int workerNum;
	private static int psNum;
	private static String clusterInfo;
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

	public static int getNodeNum() {
		return nodeNum;
	}

	public static void setNodeNum(int nodeNum) {
		MLMeta.nodeNum = nodeNum;
	}

	public static int getworkerNum() {
		return workerNum;
	}

	public static void setworkerNum(int workerNum) {
		MLMeta.workerNum = workerNum;
	}

	public static int getPsNum() {
		return psNum;
	}

	public static void setPsNum(int psNum) {
		MLMeta.psNum = psNum;
	}

	public static String getClusterInfo() {
		return clusterInfo;
	}

	public static void setClusterInfo(String clusterInfo) {
		MLMeta.clusterInfo = clusterInfo;
	}
}
