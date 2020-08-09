package org.apache.flink.table.runtime.ml.python.mlframework.operator;

import org.apache.flink.table.runtime.ml.python.mlframework.event.OperatorRegisterEvent;
import org.apache.flink.table.runtime.ml.python.mlframework.event.ClusterInfoEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;

import java.util.Random;

public class MLOperator extends AbstractStreamOperator<Integer>
	implements OneInputStreamOperator<Integer, Integer>, OperatorEventHandler {

	private final String name;
	private transient OperatorEventGateway eventGateway;
	private final String ip;
	private final int port;
	public static boolean isRunning;

	public MLOperator(String name) throws Exception {
		this.name = name;
		this.ip = "192.168.1.2";
		this.port = Math.abs(new Random().nextInt() % 1024);
		isRunning = false;
//		this.ip = IpHostUtil.getIpAddress();
//		this.port = IpHostUtil.getFreePort();
	}

	@Override
	public void open() throws Exception {
		super.open();
		// sending address to coordinator
		Preconditions.checkNotNull(eventGateway, "Operator event gateway hasn't been set");
		OperatorEvent operatorRegisterEvent = new OperatorRegisterEvent(name, ip+port);
		eventGateway.sendEventToCoordinator(operatorRegisterEvent);
		System.out.println(name + " Operator send:  " + ip + ": " + port);
		// 启动python进程
		Thread operatortask = new Thread(new MLOperatorTask(name, eventGateway));
		operatortask.start();
	}

	@Override
	public void close() throws Exception {
		System.out.println(name + "  close");
		super.close();
	}

	@Override
	public void processElement(StreamRecord<Integer> element) throws Exception {
		output.collect(element);
	}

	public void setOperatorEventGateway(OperatorEventGateway eventGateway) {
		this.eventGateway = eventGateway;
	}

	@Override
	public void handleOperatorEvent(OperatorEvent evt) {
		if(evt instanceof ClusterInfoEvent) {
			String str = ((ClusterInfoEvent) evt).getCluster();
			System.out.println(name + " Operator "+ ip + ": " + port + "   get :" + str);
			isRunning = true;
		}
	}
}
