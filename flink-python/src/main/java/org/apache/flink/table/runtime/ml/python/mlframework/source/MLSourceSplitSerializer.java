package org.apache.flink.table.runtime.ml.python.mlframework.source;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.util.InstantiationUtil;

import java.io.IOException;

public class MLSourceSplitSerializer implements SimpleVersionedSerializer<MLSourceSplit> {

	@Override
	public int getVersion() {
		return 0;
	}

	@Override
	public byte[] serialize(MLSourceSplit split) throws IOException {
		return InstantiationUtil.serializeObject(split);
	}

	@Override
	public MLSourceSplit deserialize(int version, byte[] serialized) throws IOException {
		try {
			return InstantiationUtil.deserializeObject(serialized, getClass().getClassLoader());
		} catch (ClassNotFoundException e) {
			throw new RuntimeException("Failed to deserialize the split.", e);
		}
	}
}
