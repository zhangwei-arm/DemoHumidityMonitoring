
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
 * Monitor the humidity values.
 * Check the warnings from a console with netcat:
 *      nc -lk 12346
 */
@SuppressWarnings("serial")
public class AvgHumidityMonitoring {

	public static ObjectMapper objectMapper = new ObjectMapper();

	public static double humidityThreshold = 60;

	public static void main(String[] args) throws Exception {

		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "kafka:9092");
		properties.setProperty("group.id", "demo-isg-avg");


		// get the execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		DataStream<String> demoStream = env
		    .addSource(new FlinkKafkaConsumer<>("humidity", new SimpleStringSchema(), properties));

		DataStream<JsonNode> generateAlarm = demoStream
			.flatMap(new ToJsonMapper())
			.timeWindowAll(Time.seconds(30))
			.reduce(new ReduceHumidityAvg())
			.filter(new AvgHumidityFilter());

		generateAlarm.writeToSocket("host.docker.internal", 12346, new InputSerizer());

		env.execute("Alarm on average humidity");
	}

	// ------------------------------------------------------------------------

	public static class ToJsonMapper implements FlatMapFunction<String, JsonNode> {

		private transient ObjectMapper jsonParser;

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

	public static class AvgHumidityFilter implements FilterFunction<JsonNode> {
		@Override
		public boolean filter(JsonNode value) throws Exception {
			System.out.println("Filtering - \n" + value.toPrettyString());
			return value.has("sumHumidity") && value.has("counter") && 
				Double.compare(value.get("sumHumidity").asDouble()/value.get("counter").asLong(), humidityThreshold) > 0;
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
			ObjectNode parentNode = objectMapper.createObjectNode();
			ObjectNode valueNode = objectMapper.createObjectNode();
			valueNode.put("avg humidity", value.get("sumHumidity").asDouble()/value.get("counter").asLong());

			parentNode.put("type", "Warning");
			parentNode.put("message", "Average humidity exceeds threshold of " + humidityThreshold);
			parentNode.set("value", valueNode);
			
			return (parentNode.toPrettyString() + "\n").getBytes();
		}
	}

	public static class ReduceHumidityAvg implements ReduceFunction<JsonNode> {
		@Override
		public JsonNode reduce(JsonNode value1, JsonNode value2) throws Exception {
			ObjectNode resultNode = objectMapper.createObjectNode();
			long counter = 0;
			double sum = 0;
			if (value1.has("counter")) {
				counter = value1.get("counter").asLong() + 1;
				sum = value1.get("sumHumidity").asDouble() + value2.get("humidity").asDouble();
			} else if (value2.has("counter")) {
				counter = value2.get("counter").asLong() + 1;
				sum = value2.get("sumHumidity").asDouble() + value1.get("humidity").asDouble();
			} else {
				sum = value1.get("humidity").asDouble() + value2.get("humidity").asDouble();
				counter = 2;
			}

			resultNode.put("sumHumidity", sum);
			resultNode.put("counter", counter);
			System.out.println(resultNode.toPrettyString());
			return resultNode;
		}
	}
}
