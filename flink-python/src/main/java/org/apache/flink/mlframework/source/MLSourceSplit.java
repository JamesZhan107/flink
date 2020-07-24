package org.apache.flink.mlframework.source;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class MLSourceSplit implements SourceSplit, Serializable {
	private final int id;

	public MLSourceSplit(int id) {
		this.id = id;
	}

	@Override
	public String splitId() {
		return Integer.toString(id);
	}

}
