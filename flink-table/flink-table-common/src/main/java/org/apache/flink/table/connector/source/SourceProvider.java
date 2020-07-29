package org.apache.flink.table.connector.source;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.table.data.RowData;

/**
 * Provider of an {@link Source} instance as a runtime implementation for {@link ScanTableSource}.
 */
@PublicEvolving
public interface SourceProvider extends ScanTableSource.ScanRuntimeProvider {

	/**
	 * Helper method for creating a static provider.
	 */
	static SourceProvider of(Source<RowData, ?, ?> source) {
		return new SourceProvider() {
			@Override
			public Source<RowData, ?, ?> createSource() {
				return source;
			}

			@Override
			public boolean isBounded() {
				return source.getBoundedness() == Boundedness.BOUNDED;
			}
		};
	}

	Source<RowData, ?, ?> createSource();
}

