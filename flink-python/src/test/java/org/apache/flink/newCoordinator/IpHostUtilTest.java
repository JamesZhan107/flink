package org.apache.flink.newCoordinator;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.mlframework.util.IpHostUtil;
import org.junit.Test;

import java.io.IOException;

public class IpHostUtilTest {
	@Test
	public static void main(String[] args) throws Exception {
		long time1 = System.currentTimeMillis();
		System.out.println(IpHostUtil.getFreePort());
		long time2 = System.currentTimeMillis();
		System.out.println(IpHostUtil.getFreeSocket());
		long time3 = System.currentTimeMillis();
		System.out.println(IpHostUtil.getIpAddress());
		long time4 = System.currentTimeMillis();
		System.out.println(time2 - time1);
		System.out.println(time3 - time2);
		System.out.println(time4 - time3);
	}

}
