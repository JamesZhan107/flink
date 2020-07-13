package org.apache.flink.mlframework.source;

import org.apache.flink.api.connector.source.SourceSplit;

import java.io.Serializable;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;


public class MLSourceSplit implements SourceSplit, Serializable {
	private final int id;
	private final BlockingQueue<Integer> records;
	private final int endIndex;
	private int index;

	public MLSourceSplit(int id) {
		this(id, 0);
	}

	public MLSourceSplit(int id, int startingIndex) {
		this(id, startingIndex, Integer.MAX_VALUE);
	}

	public MLSourceSplit(int id, int startingIndex, int endIndex) {
		this.id = id;
		this.endIndex = endIndex;
		this.index = startingIndex;
		this.records = new LinkedBlockingQueue<>();
	}

	@Override
	public String splitId() {
		return Integer.toString(id);
	}

	public int index() {
		return index;
	}

	public int endIndex() {
		return endIndex;
	}

	public boolean isAvailable() {
		return !isFinished() && !records.isEmpty();
	}

	public boolean isFinished() {
		return index == endIndex;
	}

	/**
	 * Get the next element. Block if asked.
	 */
	public int[] getNext(boolean blocking) throws InterruptedException {
		Integer value = blocking ? records.take() : records.poll();
		return value == null ? null : new int[]{value, index++};
	}

	/**
	 * Add a record to this split.
	 */
	public void addRecord(int record) {
		if (!records.offer(record)) {
			throw new IllegalStateException("Failed to add record to split.");
		}
	}

	@Override
	public int hashCode() {
		return Objects.hash(id, Arrays.hashCode(records.toArray(new Integer[0])), endIndex, index);
	}

	@Override
	public boolean equals(Object obj) {
		if (!(obj instanceof MLSourceSplit)) {
			return false;
		}
		MLSourceSplit that = (MLSourceSplit) obj;
		return
			id == that.id &&
				index == that.index &&
				Arrays.equals(records.toArray(new Integer[0]), that.records.toArray(new Integer[0])) &&
				endIndex == that.endIndex;
	}

	@Override
	public String toString() {
		return String.format("MLSourceSplit(id=%d, num_records=%d, endIndex=%d, currentIndex=%d)",
			id, records.size(), endIndex, index);
	}
}
