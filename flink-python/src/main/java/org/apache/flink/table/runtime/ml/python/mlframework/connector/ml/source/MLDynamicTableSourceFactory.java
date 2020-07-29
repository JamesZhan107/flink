package org.apache.flink.table.runtime.ml.python.mlframework.connector.ml.source;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;

import java.util.Map;
import java.util.Set;

public class MLDynamicTableSourceFactory implements DynamicTableSourceFactory {
	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		Map<String, String> rawProperties = context.getCatalogTable().getOptions();
		Configuration properties = new Configuration();
		for (String key : rawProperties.keySet()) {
			properties.setString(key, rawProperties.get(key));
		}

		DescriptorProperties descriptorProperties = new DescriptorProperties();
		descriptorProperties.putProperties(rawProperties);
		descriptorProperties.putTableSchema("schema", context.getCatalogTable().getSchema());
		MLScanTableSource tableSource = new MLScanTableSource(descriptorProperties, context.getCatalogTable().getSchema(), null);
		return tableSource;

	}

	@Override
	public String factoryIdentifier() {
		return "ml";
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		return null;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		return null;
	}
}
