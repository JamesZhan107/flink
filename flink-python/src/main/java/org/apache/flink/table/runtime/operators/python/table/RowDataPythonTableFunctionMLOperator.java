package org.apache.flink.table.runtime.operators.python.table;

import org.apache.calcite.rel.core.JoinRelType;
import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.runtime.ml.python.mlframework.event.ClusterInfoEvent;
import org.apache.flink.table.runtime.ml.python.mlframework.event.operatorRegisterEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.coordination.OperatorEventHandler;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.JoinedRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.functions.python.PythonFunctionInfo;
import org.apache.flink.table.planner.codegen.CodeGeneratorContext;
import org.apache.flink.table.planner.codegen.ProjectionCodeGenerator;
import org.apache.flink.table.runtime.generated.GeneratedProjection;
import org.apache.flink.table.runtime.generated.Projection;
import org.apache.flink.table.runtime.typeutils.PythonTypeUtils;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.types.logical.RowType;

import java.util.Random;

/**
 * The Python {@link TableFunction} ML operator for the blink planner.
 */
@Internal
public class RowDataPythonTableFunctionMLOperator
	extends AbstractPythonTableFunctionOperator<RowData, RowData, RowData> implements OperatorEventHandler {


	private static final long serialVersionUID = 1L;

	/**
	 * The collector used to collect records.
	 */
	private transient StreamRecordRowDataWrappingCollector rowDataWrapper;

	/**
	 * The JoinedRowData reused holding the execution result.
	 */
	private transient JoinedRowData reuseJoinedRow;

	/**
	 * The Projection which projects the udtf input fields from the input row.
	 */
	private transient Projection<RowData, BinaryRowData> udtfInputProjection;

	/**
	 * The TypeSerializer for udtf execution results.
	 */
	private transient TypeSerializer<RowData> udtfOutputTypeSerializer;

	/**
	 * The TypeSerializer for udtf input elements.
	 */
	private transient TypeSerializer<RowData> udtfInputTypeSerializer;

	/**
	 * The type serializer for the forwarded fields.
	 */
	private transient RowDataSerializer forwardedInputSerializer;

	private transient String name;
	private transient OperatorEventGateway eventGateway;
	private transient String ip;
	private transient int port;

	public RowDataPythonTableFunctionMLOperator(
		Configuration config,
		PythonFunctionInfo tableFunction,
		RowType inputType,
		RowType outputType,
		int[] udtfInputOffsets,
		JoinRelType joinType) {
		super(config, tableFunction, inputType, outputType, udtfInputOffsets, joinType);
	}

	@Override
	@SuppressWarnings("unchecked")
	public void open() throws Exception {
		super.open();
		rowDataWrapper = new StreamRecordRowDataWrappingCollector(output);
		reuseJoinedRow = new JoinedRowData();

		udtfInputProjection = createUdtfInputProjection();
		forwardedInputSerializer = new RowDataSerializer(inputType);
		udtfInputTypeSerializer = PythonTypeUtils.toBlinkTypeSerializer(userDefinedFunctionInputType);
		udtfOutputTypeSerializer = PythonTypeUtils.toBlinkTypeSerializer(userDefinedFunctionOutputType);

		ip = "192.168.1.2";
		port = Math.abs(new Random().nextInt() % 1024);
		OperatorEvent operatorRegisterEvent = new operatorRegisterEvent("mlOperator", ip, port);
		eventGateway.sendEventToCoordinator(operatorRegisterEvent);
		System.out.println(ip + ":" + port);
	}

	@Override
	public void bufferInput(RowData input) {
		// always copy the input RowData
		RowData forwardedFields = forwardedInputSerializer.copy(input);
		forwardedFields.setRowKind(input.getRowKind());
		forwardedInputQueue.add(forwardedFields);
	}

	@Override
	public RowData getFunctionInput(RowData element) {
		return udtfInputProjection.apply(element);
	}

	@Override
	public void processElementInternal(RowData value) throws Exception {
		udtfInputTypeSerializer.serialize(getFunctionInput(value), baosWrapper);
		pythonFunctionRunner.process(baos.toByteArray());
		baos.reset();
	}

	private Projection<RowData, BinaryRowData> createUdtfInputProjection() {
		final GeneratedProjection generatedProjection = ProjectionCodeGenerator.generateProjection(
			CodeGeneratorContext.apply(new TableConfig()),
			"UdtfInputProjection",
			inputType,
			userDefinedFunctionInputType,
			userDefinedFunctionInputOffsets);
		// noinspection unchecked
		return generatedProjection.newInstance(Thread.currentThread().getContextClassLoader());
	}

	@Override
	@SuppressWarnings("ConstantConditions")
	public void emitResult(Tuple2<byte[], Integer> resultTuple) throws Exception {
		RowData input = forwardedInputQueue.poll();
		byte[] rawUdtfResult;
		int length;
		boolean isFinishResult;
		boolean hasJoined = false;
		do {
			rawUdtfResult = resultTuple.f0;
			length = resultTuple.f1;
			isFinishResult = isFinishResult(rawUdtfResult, length);
			if (!isFinishResult) {
				reuseJoinedRow.setRowKind(input.getRowKind());
				bais.setBuffer(rawUdtfResult, 0, rawUdtfResult.length);
				RowData udtfResult = udtfOutputTypeSerializer.deserialize(baisWrapper);
				rowDataWrapper.collect(reuseJoinedRow.replace(input, udtfResult));
				resultTuple = pythonFunctionRunner.pollResult();
				hasJoined = true;
			} else if (joinType == JoinRelType.LEFT && !hasJoined) {
				GenericRowData udtfResult = new GenericRowData(userDefinedFunctionOutputType.getFieldCount());
				for (int i = 0; i < udtfResult.getArity(); i++) {
					udtfResult.setField(i, null);
				}
				rowDataWrapper.collect(reuseJoinedRow.replace(input, udtfResult));
			}
		} while (!isFinishResult);
	}

	public void setOperatorEventGateway(OperatorEventGateway eventGateway) {
		this.eventGateway = eventGateway;
	}

	@Override
	public void handleOperatorEvent(OperatorEvent evt) {
		if(evt instanceof ClusterInfoEvent) {
			String str = ((ClusterInfoEvent) evt).getCluster();
			System.out.println(name + " Operator "+ ip + ": " + port + "   get :" + str);
		}
	}
}
