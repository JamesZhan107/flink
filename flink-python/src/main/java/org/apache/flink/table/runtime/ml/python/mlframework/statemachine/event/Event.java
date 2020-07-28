package org.apache.flink.table.runtime.ml.python.mlframework.statemachine.event;

public interface Event<TYPE extends Enum<TYPE>> {
	TYPE getType();

	long getTimestamp();

	String toString();
}
