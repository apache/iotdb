package cn.edu.thu.tsfiledb.engine.bufferwrite;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.BiFunction;

import org.json.JSONArray;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import cn.edu.thu.tsfile.common.conf.TSFileConfig;
import cn.edu.thu.tsfile.common.conf.TSFileDescriptor;
import cn.edu.thu.tsfile.common.constant.JsonFormatConstant;
import cn.edu.thu.tsfile.common.utils.Pair;
import cn.edu.thu.tsfile.common.utils.RandomAccessOutputStream;
import cn.edu.thu.tsfile.common.utils.TSRandomAccessFileWriter;
import cn.edu.thu.tsfile.timeseries.write.TSRecordWriteSupport;
import cn.edu.thu.tsfile.timeseries.write.WriteSupport;
import cn.edu.thu.tsfile.timeseries.write.exception.WriteProcessException;
import cn.edu.thu.tsfile.timeseries.write.record.TSRecord;
import cn.edu.thu.tsfile.timeseries.write.schema.FileSchema;
import cn.edu.thu.tsfiledb.exception.PathErrorException;
import cn.edu.thu.tsfiledb.exception.ProcessorException;
import cn.edu.thu.tsfiledb.metadata.ColumnSchema;
import cn.edu.thu.tsfiledb.metadata.MManager;

/**
 * BufferWrite manager manage a list of bufferwrite processor
 *
 * @author kangrong
 * @author liukun
 */
public class BufferWriteManager extends LRUManager<BufferWriteProcessor> {
	private static Logger LOG = LoggerFactory.getLogger(BufferWriteManager.class);
	private static TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
	protected static BufferWriteManager instance = new BufferWriteManager(conf.maxBufferWriteNodeNum,
			MManager.getInstance(), conf.BufferWriteDir);

	public static BufferWriteManager getInstance() {
		return instance;
	}

	/**
	 * Initialize the instance of BufferWriteManager using special parameters
	 * 
	 * @param maxLRUNumber
	 * @param mManager
	 * @param normalDataDir
	 */
	public synchronized static void init(int maxLRUNumber, MManager mManager, String normalDataDir) {
		instance = new BufferWriteManager(maxLRUNumber, mManager, normalDataDir);
	}

	private BufferWriteManager(int maxLRUNumber, MManager mManager, String normalDataDir) {
		super(maxLRUNumber, mManager, normalDataDir);
	}

	/*
	 * If the buffer write processor is not exist before, it will be constructed
	 * using name space path and key-value information(args)
	 */
	@Override
	protected BufferWriteProcessor constructNewProcessor(String namespacePath, Map<String, Object> args)
			throws ProcessorException, IOException, WriteProcessException {
		long startTime = (Long) args.get(FileNodeConstants.TIMESTAMP_KEY);
		Pair<String, String> fileNamePair = prepareMeasurePathFileName.apply(namespacePath,
				Long.valueOf(startTime).toString());
		TSRecordWriterParameter param = prepareTSRecordWriterParameter.apply(namespacePath, mManager, fileNamePair);
		BufferWriteIOWriter bufferTSFileWriter = new BufferWriteIOWriter(param.fileSchema, param.outputStream);
		LOG.info("Construct a new buffer write processor, fileName:{}", fileNamePair);
		TSFileConfig conf = TSFileDescriptor.getInstance().getConfig();
		return new BufferWriteProcessor(conf, bufferTSFileWriter, param.writeSupport, param.fileSchema, namespacePath,
				fileNamePair);
	}

	@Override
	protected void initProcessor(BufferWriteProcessor processor, String namespacePath, Map<String, Object> args)
			throws ProcessorException {
		if (processor.getCloseAction() == null && args == null) {
			LOG.error(
					"The buffer write processor is not be initialized before, but also can't be initailized because of lacking of args");
			throw new RuntimeException(
					"The buffer write processor is not be initialized before, but also can't be initailized because of lacking of args");
		}
		if (args != null && args.containsKey(FileNodeConstants.CLOSE_ACTION))
			processor.setCloseAction((Action) args.get(FileNodeConstants.CLOSE_ACTION));

	}

	/**
	 * @param namespacePath
	 *            -this path is a concept in name space tree. like
	 *            root.laptop.xxx. The absolute file path is deltaDataDir +
	 *            namespacepath + number.
	 * @param MManager
	 * @param Pair<String,String>
	 * 
	 * @return TSRecordWriterParameter
	 */
	public TriFunction<String, MManager, Pair<String, String>, TSRecordWriterParameter> prepareTSRecordWriterParameter = (
			namespacePath, inputMManager, filePathPair) -> {
		Optional<List<ColumnSchema>> meaSchema;
		
		String deltaObjectType;
		try {
			deltaObjectType = mManager.getDeltaObjectTypeByPath(namespacePath);
			meaSchema = Optional.ofNullable(inputMManager.getSchemaForOneType(deltaObjectType));
		} catch (PathErrorException e) {
			throw new RuntimeException(e);
		}
		FileSchema fSchema = null;
		try {
			if (meaSchema == null) {
				throw new ProcessorException("Measures schema list is null");
			}
			fSchema = getFileSchemaFromColumnSchema(meaSchema.orElse(new ArrayList<ColumnSchema>()), deltaObjectType);
		} catch (ProcessorException e) {
			LOG.error("The measures schema list is {}", meaSchema);
			throw new RuntimeException("Get the File schema failed");
		}
		File file = new File(filePathPair.left);
		WriteSupport<TSRecord> writeSupport = new TSRecordWriteSupport();
		TSRandomAccessFileWriter outputStream;
		try {
			outputStream = new RandomAccessOutputStream(file);
		} catch (IOException e) {
			LOG.error("Initialized the data output stream failed, the file path is {}", filePathPair.left);
			throw new RuntimeException(new ProcessorException(
					"IOException: " + e.getMessage() + " create data output stream failed:" + file));
		}
		return new TSRecordWriterParameter(fSchema, outputStream, writeSupport);
	};

	/**
	 * @param namespacePath
	 *            -this path is a concept in name space tree. like
	 *            root.laptop.xxx. The absolute file path is deltaDataDir +
	 *            namespacepath + number.
	 * @param filename
	 *            - start time for this file
	 * @return Pair<String,String> left - data file right -error data file
	 */
	public BiFunction<String, String, Pair<String, String>> prepareMeasurePathFileName = (namespacePath, fileName) -> {
		String absolutePathDir = normalDataDir + namespacePath;
		File dir = new File(absolutePathDir);
		if (!dir.exists()) {
			dir.mkdirs();
		}
		String absolutePath = dir.getAbsolutePath() + File.separator + fileName;

		return new Pair<>(absolutePath, "null");
	};

	/**
	 * Construct one FileSchema from a list of columnschema and the deltaobject
	 * type
	 * 
	 * @param schemaList
	 * @param deltaObjectType
	 * @return
	 */
	private FileSchema getFileSchemaFromColumnSchema(List<ColumnSchema> schemaList, String deltaObjectType) throws WriteProcessException {
		JSONArray rowGroup = new JSONArray();

		for (ColumnSchema col : schemaList) {
			JSONObject measurement = new JSONObject();
			measurement.put(JsonFormatConstant.MEASUREMENT_UID, col.name);
			measurement.put(JsonFormatConstant.DATA_TYPE, col.dataType.toString());
			measurement.put(JsonFormatConstant.MEASUREMENT_ENCODING, col.encoding.toString());
			for (Entry<String, String> entry : col.getArgsMap().entrySet()) {
				if (JsonFormatConstant.ENUM_VALUES.equals(entry.getKey())) {
					String[] valueArray = entry.getValue().split(",");
					measurement.put(JsonFormatConstant.ENUM_VALUES, new JSONArray(valueArray));
				} else
					measurement.put(entry.getKey(), entry.getValue().toString());
			}
			rowGroup.put(measurement);
		}
		JSONObject jsonSchema = new JSONObject();
		jsonSchema.put(JsonFormatConstant.JSON_SCHEMA, rowGroup);
		jsonSchema.put(JsonFormatConstant.DELTA_TYPE, deltaObjectType);
		return new FileSchema(jsonSchema);
	}
}
