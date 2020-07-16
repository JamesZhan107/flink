package org.apache.flink.mlframework.statemachine.event;

public interface Event<TYPE extends Enum<TYPE>> {
	TYPE getType();

	long getTimestamp();

	String toString();
}
