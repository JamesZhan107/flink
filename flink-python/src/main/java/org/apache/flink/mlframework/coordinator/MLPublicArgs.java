package org.apache.flink.mlframework.coordinator;

public class MLPublicArgs {
	//单例模式，保证coordinator和enumerator共享同一Args

	private static final MLPublicArgs mlPublicArgs = new MLPublicArgs();
	private static boolean workDone;

	private MLPublicArgs() {

	}

	public static MLPublicArgs getMlPublicArgs(){
		return mlPublicArgs;
	}

	public static boolean isWorkDone() {
		return workDone;
	}

	public static void setWorkDone(boolean workDone) {
		MLPublicArgs.workDone = workDone;
	}
}
