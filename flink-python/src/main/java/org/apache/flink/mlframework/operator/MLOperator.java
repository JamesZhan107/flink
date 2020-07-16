package org.apache.flink.mlframework.operator;

import org.apache.flink.mlframework.event.AddressRegistrationEvent;
import org.apache.flink.mlframework.event.ClusterInfoEvent;
import org.apache.flink.mlframework.util.IpHostUtil;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.Preconditions;
import java.net.InetSocketAddress;
import java.util.Random;

public class MLOperator extends AbstractStreamOperator<Integer>
	implements OneInputStreamOperator<Integer, Integer>, OperatorEventHandler {

	private String name;
	private transient OperatorEventGateway eventGateway;
	private InetSocketAddress address;

	public MLOperator(String name) {
		this.name = name;
	}

	@Override
	public void open() throws Exception {
		super.open();

		// sending address to coordinator
		Preconditions.checkNotNull(eventGateway, "Operator event gateway hasn't been set");
		//address = new InetSocketAddress(IpHostUtil.getIpAddress(), IpHostUtil.getFreePort());
		address = new InetSocketAddress("192.168.1.2", Math.abs(new Random().nextInt() % 1024));
		OperatorEvent addressEvent = new AddressRegistrationEvent(name, address);
		eventGateway.sendEventToCoordinator(addressEvent);
		System.out.println(name + " Operator send:  " + address);

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
		//output.collect(element);
	}

	public void setOperatorEventGateway(OperatorEventGateway eventGateway) {
		this.eventGateway = eventGateway;
	}

	@Override
	public void handleOperatorEvent(OperatorEvent evt) {
		if(evt instanceof ClusterInfoEvent) {
			String str = ((ClusterInfoEvent) evt).getCluster();
			System.out.println(name + " Operator "+ address.toString() + "   get :" + str);
		}
	}
}
