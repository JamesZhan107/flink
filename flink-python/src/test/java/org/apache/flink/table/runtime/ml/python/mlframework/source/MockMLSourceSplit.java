package org.apache.flink.table.runtime.ml.python.mlframework.source;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;


public class MockMLSourceSplit implements SourceSplit, Serializable {
	private final int id;

	public MockMLSourceSplit(int id) {
		this.id = id;
	}

	@Override
	public String splitId() {
		return Integer.toString(id);
	}

}
