package io.s4.latin.core;

import io.s4.dispatcher.EventDispatcher;
import io.s4.latin.pojo.StreamRow;
import io.s4.latin.pojo.StreamRow.ValueType;

import java.util.Date;
import java.util.Properties;

import org.pentaho.di.core.BlockingRowSet;
import org.pentaho.di.core.KettleEnvironment;
import org.pentaho.di.core.exception.KettleException;
import org.pentaho.di.core.exception.KettleStepException;
import org.pentaho.di.core.row.RowDataUtil;
import org.pentaho.di.core.row.RowMeta;
import org.pentaho.di.core.row.RowMetaInterface;
import org.pentaho.di.core.row.ValueMeta;
import org.pentaho.di.core.row.ValueMetaInterface;
import org.pentaho.di.trans.RowProducer;
import org.pentaho.di.trans.Trans;
import org.pentaho.di.trans.TransMeta;
import org.pentaho.di.trans.step.RowListener;
import org.pentaho.di.trans.step.StepInterface;

public class KettlePE extends AbstractLatinPE {

	private String id;
	private EventDispatcher dispatcher;
	private String outputStreamName;
	private boolean debug = false;
	private boolean first = true;

	private String transFile = "test";
	private Trans transformation;
	private RowMeta rowMeta;
	private RowProducer rowProducer;
	private String inputStep = "input";
	private String outputStep;

	private BlockingRowSet rsi;
	private int logInterval = 10;

	public KettlePE(Properties props) {
		super();

		if (props != null) {
			if (props.getProperty("stream") != null) {
				String streamName = props.getProperty("stream");
				setOutputStreamName(streamName);
			}
			if (props.getProperty("transformation") != null) {
				transFile = props.getProperty("transformation");
			}
			if (props.getProperty("input") != null) {
				inputStep = props.getProperty("input");
			}
			if (props.getProperty("output") != null) {
				outputStep = props.getProperty("output");
			}

			if (props.getProperty("loginterval") != null) {
				try {
					logInterval = Integer.parseInt(props.getProperty("loginterval"));
				}
				catch (Exception e) {
					e.printStackTrace();
				}
			}
			if (props.getProperty("debug") != null) {
				debug = Boolean.parseBoolean(props.getProperty("debug"));
			}

			init();


		}
	}
	private void init() {
		try {
			KettleEnvironment.init();
			transformation = new Trans(new TransMeta(transFile));
			//			transformation.prepareExecution(null);
		} catch (KettleException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
	public EventDispatcher getDispatcher() {
		return dispatcher;
	}

	public void setDispatcher(EventDispatcher dispatcher) {
		this.dispatcher = dispatcher;
	}

	public String getOutputStreamName() {
		return outputStreamName;
	}

	public void setOutputStreamName(String outputStreamName) {
		this.outputStreamName = outputStreamName;
	}

	public void setTransFile(String file) {
		this.transFile = file;
	}

	public void setId(String id) {
		this.id = id;
	}

	public boolean isDebug() {
		return debug;
	}

	public void setDebug(boolean debug) {
		this.debug  = debug;
	}


	public void processEvent(StreamRow row) {
		try {
			if (row == null) {
				return;
			}

			if (first) {
				rsi = new BlockingRowSet(transformation.getTransMeta().getSizeRowset());

				transformation.prepareExecution(null);				
				rowProducer = transformation.addRowProducer(inputStep, 0);
				rowMeta = createRowMeta(row);
				first = false;
				new Thread(new Runnable() {

					@Override
					public void run() {
						try {
							if (logInterval > 0) {
								Long start = (new Date()).getTime();
								while (!Thread.interrupted()) {
									Thread.sleep(logInterval * 1000);
									Long curr = (new Date()).getTime();
									Long diff = (curr - start) / 1000;
									transformation.printStats(diff.intValue());
								}
							}
						}
						catch (Exception e) {
							e.printStackTrace();
						}
					}
				}).start();

				if (outputStep != null) {
					StepInterface spi = transformation.getStepInterface(outputStep, 0);
					spi.addRowListener(new RowListener() {

						@Override
						public void rowWrittenEvent(RowMetaInterface rm, Object[] r)
						throws KettleStepException {
							StreamRow row = convert(rm, r);
							dispatcher.dispatchEvent(outputStreamName, row);
						}

						@Override
						public void rowReadEvent(RowMetaInterface arg0, Object[] arg1)
						throws KettleStepException {
							// TODO Auto-generated method stub

						}

						@Override
						public void errorRowWrittenEvent(RowMetaInterface arg0, Object[] arg1)
						throws KettleStepException {
							// TODO Auto-generated method stub

						}

					});
					
				}

				transformation.startThreads();
				System.out.println(rowMeta);
			}
			Object[] rowData = RowDataUtil.allocateRowData(row.getKeys().size());
			int index = 0;

			for (String name : rowMeta.getFieldNames()) {
				rowData[index] = row.get(name);
				index++;
			}

			rowProducer.putRow(rowMeta, rowData);


			//	        System.out.println(rowData);

			//			if (debug) {
			//				System.out.println(sr);
			//			}
			//			dispatcher.dispatchEvent(outputStreamName, sr);
		} catch (Exception e) {
			transformation.printStats(10);
			e.printStackTrace();
		}

	}



	@Override
	public void output() {
		// TODO Auto-generated method stub

	}

	@Override
	public String getId() {
		return this.id;
	}

	public StreamRow convert(RowMetaInterface meta, Object[] objects) {
		StreamRow row = new StreamRow();
		int index = 0;
		for (String key : meta.getFieldNames()) {
			ValueMetaInterface vi = meta.getValueMeta(index);
			ValueType vt = ValueType.OBJECT;
			switch (vi.getType()) {
			case ValueMetaInterface.TYPE_BOOLEAN:
				vt = ValueType.BOOLEAN;
				break;
			case ValueMetaInterface.TYPE_BIGNUMBER:
				vt = ValueType.NUMBER;
				break;
			case ValueMetaInterface.TYPE_INTEGER:
				vt = ValueType.INTEGER;
				break;
			case ValueMetaInterface.TYPE_NUMBER:
				vt = ValueType.NUMBER;
				break;
			case ValueMetaInterface.TYPE_STRING:
				vt = ValueType.STRING;
				break;
			case ValueMetaInterface.TYPE_DATE:
				vt = ValueType.DATE;
				break;
			}
			row.set(key, objects[index], vt);
			index++;
		}
		return row;
	}
	public RowMeta createRowMeta(StreamRow row) {
		RowMeta outputRowMeta = new RowMeta();

		for (String key : row.getKeys())
		{
			int type = ValueMetaInterface.TYPE_STRING;
			switch (row.getValueMeta(key)) {
			case BOOLEAN:
				type = ValueMetaInterface.TYPE_BOOLEAN;
				break;
			case STRING:
				type = ValueMetaInterface.TYPE_STRING;
				break;
			case NUMBER:
				type = ValueMetaInterface.TYPE_NUMBER;
				break;
			case INTEGER:
				type = ValueMetaInterface.TYPE_INTEGER;
				break;
			case OBJECT:
				type = ValueMetaInterface.TYPE_NONE;
				break;


			}

			ValueMetaInterface valueMeta=new ValueMeta(key,type);

			outputRowMeta.addValueMeta(valueMeta);

		}
		return outputRowMeta;
	}


}
