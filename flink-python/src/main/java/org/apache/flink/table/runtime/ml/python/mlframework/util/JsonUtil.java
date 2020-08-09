package org.apache.flink.table.runtime.ml.python.mlframework.util;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonInclude;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.DeserializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.SerializationFeature;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;

import java.io.IOException;

public class JsonUtil {
	// 默认jackson对象
	private static ObjectMapper defaultObjectMapper = new ObjectMapper();

	//根据项目需要自定义配置
	static {
		defaultObjectMapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false)//解决SerializationFeature.FAIL_ON_EMPTY_BEANS异常
			.setSerializationInclusion(JsonInclude.Include.NON_NULL)//属性值为null的不参与序列化
			.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);//反序列化时忽略对象中不存在的json字段
	}

	//将字符串转成对象
	public static <T> T parseObject(String text, Class<T> clazz) {
		T obj = null;
		if (!StringUtils.isEmpty(text)) {
			try {
				obj = defaultObjectMapper.readValue(text, clazz);
			} catch (IOException e) {
				System.out.println(e.getMessage());
			}
		}
		return obj;
	}

	//字符串转ObjectNode
	public static ObjectNode toObjectNode(String text) {
		ObjectNode objectNode = null;
		if (!StringUtils.isEmpty(text)) {
			try {
				objectNode = (ObjectNode) defaultObjectMapper.readTree(text);
			} catch (IOException e) {
				System.out.println(e.getMessage());
			}
		}
		return objectNode;
	}

	//对象转json
	public static String toJSONString(Object object) {
		String res = null;
		try {
			res = defaultObjectMapper.writeValueAsString(object);
		} catch (JsonProcessingException e) {
			System.out.println(e.getMessage());
		}
		return res;
	}
}
