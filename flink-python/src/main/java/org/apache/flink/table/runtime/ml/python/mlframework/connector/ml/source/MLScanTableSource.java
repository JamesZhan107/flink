package org.apache.flink.table.runtime.ml.python.mlframework.connector.ml.source;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceProvider;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.runtime.ml.python.mlframework.source.MLSource;

import java.util.Properties;

public class MLScanTableSource implements ScanTableSource {
	private final DescriptorProperties properties;
	private final TableSchema schema;

	private final Properties mlProperties;

	public MLScanTableSource(DescriptorProperties properties, TableSchema schema, Properties mlProperties) {
		this.properties = properties;
		this.schema = schema;
		this.mlProperties = mlProperties;
	}

	@Override
	public ChangelogMode getChangelogMode() {
		return ChangelogMode.insertOnly();
	}

	@Override
	public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
		return SourceProvider.of(new MLSource<RowData>(Boundedness.CONTINUOUS_UNBOUNDED));
	}

	@Override
	public DynamicTableSource copy() {
		return new MLScanTableSource(properties, schema, mlProperties);
	}

	@Override
	public String asSummaryString() {
		return "ml";
	}
}
