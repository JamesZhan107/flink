package org.apache.flink.mlframework.coordinator;

import org.apache.flink.mlframework.event.AddressRegistrationEvent;
import org.apache.flink.mlframework.event.ClusterInfoEvent;
import org.apache.flink.mlframework.event.StopOperatorEvent;
import org.apache.flink.mlframework.event.WorkDoneEvent;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

class MLCoordinator implements OperatorCoordinator {
	private volatile static MLCoordinator mlCoordinator;
	private String clusters = "";
	private int nodeNum;
	private List<Context> contextList;

	public static MLCoordinator getCoordinator(Context context) {
		if (mlCoordinator == null) {
			synchronized (MLCoordinator.class) {
				if (mlCoordinator == null) {
					List<Context> list = new ArrayList<>();
					list.add(context);
					mlCoordinator = new MLCoordinator(list);
				}
			}
		} else {
			mlCoordinator.contextList.add(context);
		}
		return mlCoordinator;
	}

	private MLCoordinator(List<Context> contextList) {
		this.contextList = contextList;
	}


	@Override
	public void start() throws Exception {

	}

	@Override
	public void close() throws Exception {

	}

	@Override
	public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
		// stop the ps through sendEvent
		/*
		if(event instanceof AddressRegistrationEvent){
			String name = ((AddressRegistrationEvent) event).getName();
			InetSocketAddress address = ((AddressRegistrationEvent) event).getAddress();
			System.out.println("coordinator get a " + name + ", the address is :  "+ address.toString());
			nodeNum++;
			System.out.println(nodeNum);
			clusters += address.toString();
			if (nodeNum== 5) {
				//Thread.sleep(500);
				for (int i = 0; i < contextList.size(); ++i) {
					Context context = contextList.get(i);
					for (int j = 0; j < context.currentParallelism(); j++) {
						context.sendEvent(new ClusterInfoEvent(clusters), j);
					}
				}
			}
		}else if(event instanceof WorkDoneEvent){
			nodeNum--;
			System.out.println("nodenum:  " + nodeNum);
			if(nodeNum == 2){
				for (int i = 0; i < contextList.size(); ++i){
					Context context = contextList.get(i);
					for (int j = 0; j < context.currentParallelism(); j++) {
						context.sendEvent(new StopOperatorEvent(false), j);
					}
				}
			}
		}
		*/

		Preconditions.checkArgument(
			event instanceof AddressRegistrationEvent, "Operator event must be a AddressRegistrationEvent");
		String name = ((AddressRegistrationEvent) event).getName();
		InetSocketAddress address = ((AddressRegistrationEvent) event).getAddress();
		System.out.println("coordinator get a " + name + ", the address is :  "+ address.toString());
		nodeNum++;
		System.out.println(nodeNum);
		clusters += address.toString();
		if (nodeNum== 5) {
			Thread.sleep(1000);
			for (int i = 0; i < contextList.size(); ++i) {
				Context context = contextList.get(i);
				for (int j = 0; j < context.currentParallelism(); j++) {
					context.sendEvent(new ClusterInfoEvent(clusters), j);
				}
			}
		}

	}

	@Override
	public void subtaskFailed(int subtask, @Nullable Throwable reason) {

	}

	@Override
	public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture) throws Exception {

	}

	@Override
	public void checkpointComplete(long checkpointId) {

	}

	@Override
	public void resetToCheckpoint(byte[] checkpointData) throws Exception {

	}
}
