
package isg;

import java.util.Properties;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.core.JsonProcessingException;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.JsonNode;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.util.Collector;

/**
 * Monitor the humidity values of a text streaming.
 * Check the warnings from a console with netcat:
 *      nc -l 12345
 */
@SuppressWarnings("serial")
public class HumidityMonitoring {

	public static ObjectMapper objectMapper = new ObjectMapper();

	public static double humidityThreshold = 60;

	public static void main(String[] args) throws Exception {

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "kafka:9092");
		properties.setProperty("group.id", "demo-isg");


		// get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> demoStream = env
		    .addSource(new FlinkKafkaConsumer<>("humidity", new SimpleStringSchema(), properties));

		// parse the data, group it, window it, and generate a warning when the value
		// exceeds the threshold
		// DataStream<JsonNode> generateAlarm = 
		// 	demoStream.flatMap(new ToJsonMapper()).filter(new HumidityFilter());

		DataStream<JsonNode> generateAlarm = demoStream
			.flatMap(new ToJsonMapper()).filter(new HumidityFilter());

		generateAlarm.writeToSocket("host.docker.internal", 12345, new InputSerizer());

		env.execute("Alarm on high humidity");
	}

	// ------------------------------------------------------------------------

	public static class ToJsonMapper implements FlatMapFunction<String, JsonNode> {

		private transient ObjectMapper jsonParser;

		/**
		 * Select the language from the incoming JSON text.
		 */
		@Override
		public void flatMap(String value, Collector<JsonNode> out) {
			if (jsonParser == null) {
				jsonParser = new ObjectMapper();
			}

			try {
				JsonNode inputNode = jsonParser.readValue(value, JsonNode.class);

				// Let's take a look at it.
				System.out.println(inputNode.toPrettyString());

				boolean isDeviceEvent = inputNode.has("type") && inputNode.get("type").asText().equals("event");
				boolean isMetadata = inputNode.has("type") && inputNode.get("type").asText().equals("metadata");

				if (isDeviceEvent && inputNode.has("value") && inputNode.get("value").has("humidity")) {
					out.collect(inputNode.get("value"));
				} else if (isMetadata) {
					if (inputNode.has("humidityThreshold")) {
						humidityThreshold = inputNode.get("humidityThreshold").asDouble();
						System.out.println("humidityThreshold changed to " + humidityThreshold);
					}
				} else {
					// Invalid inputs, dump to stdout.
					System.out.println("WARNING: Invalid input: \n" + value);
				}
			} catch (JsonProcessingException e) {
				e.printStackTrace();
			}
		}

	}

	public static class HumidityFilter implements FilterFunction<JsonNode> {
		@Override
		public boolean filter(JsonNode value) throws Exception {
			System.out.println("Filtering - \n" + value.toPrettyString());
			return value.has("humidity") && Double.compare(value.get("humidity").asDouble(), humidityThreshold) > 0;
		}
	}

	public static class InputSinker implements SinkFunction<JsonNode> {
	    @Override
		public void invoke(JsonNode value, Context context) throws Exception {
			System.out.println(value.toPrettyString());
		}

	}

	public static class InputSerizer implements SerializationSchema<JsonNode> {
		@Override
		public byte[] serialize(JsonNode value) {
			//ObjectMapper objectMapper = new ObjectMapper();
			ObjectNode parentNode = objectMapper.createObjectNode();


			parentNode.put("type", "Warning");
			parentNode.put("message", "Humidity exceeds threshold of " + humidityThreshold);
			parentNode.set("value", value);
			
			return (parentNode.toPrettyString() + "\n").getBytes();
		}
	}

	public static class ReduceHumidityAvg implements ReduceFunction<JsonNode> {
		@Override
		public JsonNode reduce(JsonNode value1, JsonNode value2) throws Exception {
			ObjectNode resultNode = objectMapper.createObjectNode();
			double sum = value1.get("humidity").asDouble() + value2.get("humidity").asDouble();

			resultNode.put("humidity", sum/2);
			return resultNode;
		}
	}
}
