package org.apache.flink.table.runtime.ml.python.mlframework.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

public class MockMLSourceSplitSerializer implements SimpleVersionedSerializer<MockMLSourceSplit> {

	@Override
	public int getVersion() {
		return 0;
	}

	@Override
	public byte[] serialize(MockMLSourceSplit split) throws IOException {
		return InstantiationUtil.serializeObject(split);
	}

	@Override
	public MockMLSourceSplit deserialize(int version, byte[] serialized) throws IOException {
		try {
			return InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Failed to deserialize the split.", e);
		}
	}
}
