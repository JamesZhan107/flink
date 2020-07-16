package org.apache.flink.mlframework.statemachine;

import org.apache.flink.mlframework.statemachine.event.MLEvent;
import org.apache.flink.mlframework.statemachine.event.MLEventType;

import java.util.concurrent.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

public abstract class AbstractMLStateMachine {
	protected final Lock writeLock;
	protected final Lock readLock;
	protected StateMachine<AMStatus, MLEventType, MLEvent> stateMachine;
	protected final BlockingQueue<MLEvent> eventQueue = new ArrayBlockingQueue<>(1000);
	protected final ExecutorService exService;

	protected AbstractMLStateMachine() {
		ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
		this.readLock = readWriteLock.readLock();
		this.writeLock = readWriteLock.writeLock();
		this.stateMachine = buildStateMachine();
		exService = Executors.newFixedThreadPool(1, r -> {
			Thread eventThread = new Thread(r);
			eventThread.setDaemon(true);
			eventThread.setName("am_event_handler");
			return eventThread;
		});
		exService.submit(new EventHandle());
	}

	class EventHandle implements Runnable {
		@Override
		public void run() {
			MLEvent event;
			while (true) {
				try {
					event = eventQueue.take();
					handle(event);
				}catch (Exception e) {
					e.printStackTrace();
				}
			}
		}
	}

	abstract protected StateMachine<AMStatus, MLEventType, MLEvent> buildStateMachine();

	public boolean sendEvent(MLEvent event) {
		try {
			return eventQueue.offer(event, 5, TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			e.printStackTrace();
			return false;
		}
	}

	public void handle(MLEvent mlEvent) throws Exception {
		try {
			writeLock.lock();
			AMStatus oldState = getInternalState();
			try {
				getStateMachine().doTransition(mlEvent.getType(), mlEvent);
			} catch (InvalidStateTransitionException e) {
				e.printStackTrace();
				System.out.println("Can't handle this event at current state");
				if (oldState != getInternalState()) {
					System.out.println("Job Transitioned from " + oldState + " to " + getInternalState());
				}
				throw e;
			}
			System.out.println("AM doTransition:" + oldState.toString() + " => " + getInternalState().toString());

		} finally {
			writeLock.unlock();
		}
	}

	public AMStatus getInternalState() {
		readLock.lock();
		try {
			return getStateMachine().getCurrentState();
		} finally {
			readLock.unlock();
		}
	}

	protected StateMachine<AMStatus, MLEventType, MLEvent> getStateMachine() {
		return this.stateMachine;
	}
}
