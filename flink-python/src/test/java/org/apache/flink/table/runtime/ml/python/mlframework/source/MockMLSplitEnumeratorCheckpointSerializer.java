package org.apache.flink.table.runtime.ml.python.mlframework.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;
import java.util.Set;

public class MockMLSplitEnumeratorCheckpointSerializer implements SimpleVersionedSerializer<Set<MockMLSourceSplit>>  {

	@Override
	public int getVersion() {
		return 0;
	}

	@Override
	public byte[] serialize(Set<MockMLSourceSplit> split) throws IOException {
		return InstantiationUtil.serializeObject(split);
	}

	@Override
	public Set<MockMLSourceSplit> deserialize(int version, byte[] serialized) throws IOException {
		try {
			return InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Failed to deserialize the split.", e);
		}
	}
}
