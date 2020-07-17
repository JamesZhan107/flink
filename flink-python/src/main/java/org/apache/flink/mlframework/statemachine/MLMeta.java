package org.apache.flink.mlframework.statemachine;

public class MLMeta {
	//单例模式，保证coordinator和enumerator共享同一Args

	private static final MLMeta ML_META = new MLMeta();
	private static boolean workDone;
	private static int nodeNum;
	private static int workerNum;
	private static int psNum;
	private static String clusterInfo;

	private MLMeta() {

	}

	public static MLMeta getMlMeta(){
		return ML_META;
	}

	public static boolean isWorkDone() {
		return workDone;
	}

	public static void setWorkDone(boolean workDone) {
		MLMeta.workDone = workDone;
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
