package qslv.kstream.enhancement;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.TopologyTestDriver;
import com.fasterxml.jackson.databind.JavaType;

import qslv.common.kafka.JacksonAvroDeserializer;
import qslv.common.kafka.JacksonAvroSerializer;
import qslv.common.kafka.ResponseMessage;
import qslv.common.kafka.TraceableMessage;
import qslv.data.Account;
import qslv.data.OverdraftInstruction;
import qslv.data.OverdraftInstructions;
import qslv.kstream.PostingResponse;
import qslv.kstream.workflow.WorkflowMessage;
import qslv.kstream.PostingRequest;

public class TestSetup {

	static public final String SCHEMA_REGISTRY = "http://localhost:8081";

	static public final String ACCOUNT_TOPIC = "account";
	static public final String OVERDRAFT_TOPIC = "overdraft";
	static public final String REQUEST_TOPIC = "posting.request";
	static public final String RESPONSE_TOPIC = "posting.response";
	static public final String ENHANCED_REQUEST_TOPIC = "enhanced.request";
	static public final String MATCH_RESERVATION_TOPIC = "match.reservation";

	private ConfigProperties configProperties = new ConfigProperties();

	private Serde<Account> accountSerde;
	private Serde<OverdraftInstruction> overdraftSerde;
	private Serde<OverdraftInstructions> overdraftCollectionSerde;
	private Serde<TraceableMessage<PostingRequest>> postingRequestSerde;
	private Serde<TraceableMessage<WorkflowMessage>> enhancedPostingRequestSerde;
	private Serde<ResponseMessage<PostingRequest, PostingResponse>> postingReplySerde;
	
	private EnhancementTopology enhancementTopology = new EnhancementTopology();

	private TopologyTestDriver testDriver;

	private TestInputTopic<String, Account> accountTopic;
	private TestInputTopic<String, OverdraftInstruction> overdraftTopic;
	private TestInputTopic<String, TraceableMessage<PostingRequest>> requestTopic;
	private TestOutputTopic<String, TraceableMessage<WorkflowMessage>> enhancedRequestTopic;
	private TestOutputTopic<UUID, TraceableMessage<WorkflowMessage>> matchReservationTopic;
	private TestOutputTopic<String, ResponseMessage<PostingRequest, PostingResponse>> responseTopic;

	public TestSetup() throws Exception {
		configProperties.setAitid("12345");
		configProperties.setAccountLogTopic(ACCOUNT_TOPIC);
		configProperties.setOverdraftLogTopic(OVERDRAFT_TOPIC);
		configProperties.setEnhancedRequestTopic(ENHANCED_REQUEST_TOPIC);
		configProperties.setPostingRequestTopic(REQUEST_TOPIC);
		configProperties.setPostingResponseTopic(RESPONSE_TOPIC);
		configProperties.setMatchReservationTopic(MATCH_RESERVATION_TOPIC);
		enhancementTopology.setConfigProperties(configProperties);

		Map<String, String> config = new HashMap<>();
		config.put("schema.registry.url", SCHEMA_REGISTRY);
		accountSerde = accountSerde(config);
		overdraftSerde = overdraftSerde(config);
		overdraftCollectionSerde = overdraftCollectionSerde(config);
		postingRequestSerde = postingRequestSerde(config);
		enhancedPostingRequestSerde = enhancedPostingRequestSerde(config);
		postingReplySerde = postingReplySerde(config);

		enhancementTopology.setAccountSerde(accountSerde);
		enhancementTopology.setOverdraftSerde(overdraftSerde);
		enhancementTopology.setOverdraftCollectionSerde(overdraftCollectionSerde);
		enhancementTopology.setPostingRequestSerde(postingRequestSerde);
		enhancementTopology.setEnhancedPostingRequestSerde(enhancedPostingRequestSerde);
		enhancementTopology.setPostingReplySerde(postingReplySerde);

		StreamsBuilder builder = new StreamsBuilder();
		enhancementTopology.kStream(builder);

		Topology topology = builder.build();

		Properties props = new Properties();
		props.put(StreamsConfig.APPLICATION_ID_CONFIG, "unit.reservation");
		props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
		testDriver = new TopologyTestDriver(topology, props);

		accountTopic = testDriver.createInputTopic(ACCOUNT_TOPIC, Serdes.String().serializer(),
				accountSerde.serializer());
		overdraftTopic = testDriver.createInputTopic(OVERDRAFT_TOPIC, Serdes.String().serializer(),
				overdraftSerde.serializer());
		requestTopic = testDriver.createInputTopic(REQUEST_TOPIC, Serdes.String().serializer(),
				postingRequestSerde.serializer());
		responseTopic = testDriver.createOutputTopic(RESPONSE_TOPIC, Serdes.String().deserializer(),
				postingReplySerde.deserializer());
		enhancedRequestTopic = testDriver.createOutputTopic(ENHANCED_REQUEST_TOPIC, Serdes.String().deserializer(),
				enhancedPostingRequestSerde.deserializer());
		matchReservationTopic = testDriver.createOutputTopic(MATCH_RESERVATION_TOPIC, Serdes.UUID().deserializer(),
				enhancedPostingRequestSerde.deserializer());
	}

	static public Serde<Account> accountSerde(Map<String, ?> config) throws Exception {
		JacksonAvroSerializer<Account> serializer = new JacksonAvroSerializer<>();
		JacksonAvroDeserializer<Account> deserializer = new JacksonAvroDeserializer<>();
		serializer.configure(config, false);
		deserializer.configure(config, false);
		return Serdes.serdeFrom(serializer, deserializer);
	}
	
	static public Serde<OverdraftInstruction> overdraftSerde(Map<String, ?> config) throws Exception {
		JacksonAvroSerializer<OverdraftInstruction> serializer = new JacksonAvroSerializer<>();
		JacksonAvroDeserializer<OverdraftInstruction> deserializer = new JacksonAvroDeserializer<>();
		serializer.configure(config, false);
		deserializer.configure(config, false);
		return Serdes.serdeFrom(serializer, deserializer);
	}	
	
	static public Serde<OverdraftInstructions> overdraftCollectionSerde(Map<String, ?> config) throws Exception {
		JacksonAvroSerializer<OverdraftInstructions> serializer = new JacksonAvroSerializer<>();
		JacksonAvroDeserializer<OverdraftInstructions> deserializer = new JacksonAvroDeserializer<>();
		serializer.configure(config, false);
		deserializer.configure(config, false);
		return Serdes.serdeFrom(serializer, deserializer);
	}	
	
	static public Serde<TraceableMessage<PostingRequest>> postingRequestSerde(Map<String, ?> config) throws Exception {
		JacksonAvroSerializer<TraceableMessage<PostingRequest>> serializer = new JacksonAvroSerializer<>();
		JacksonAvroDeserializer<TraceableMessage<PostingRequest>> deserializer = new JacksonAvroDeserializer<>();
		JavaType type = serializer.getTypeFactory().constructParametricType(TraceableMessage.class, PostingRequest.class);
		serializer.configure(config, false, type);
		deserializer.configure(config, false);
		return Serdes.serdeFrom(serializer, deserializer);
	}
	
	static public Serde<TraceableMessage<WorkflowMessage>> enhancedPostingRequestSerde(Map<String, ?> config) throws Exception {
		JacksonAvroSerializer<TraceableMessage<WorkflowMessage>> serializer = new JacksonAvroSerializer<>();
		JacksonAvroDeserializer<TraceableMessage<WorkflowMessage>> deserializer = new JacksonAvroDeserializer<>();
		JavaType type = serializer.getTypeFactory().constructParametricType(TraceableMessage.class, WorkflowMessage.class);
		serializer.configure(config, false, type);
		deserializer.configure(config, false);
		return Serdes.serdeFrom(serializer, deserializer);
	}	

	static public Serde<ResponseMessage<PostingRequest, PostingResponse>> postingReplySerde(Map<String, ?> config) throws Exception {
		JacksonAvroSerializer<ResponseMessage<PostingRequest, PostingResponse>> serializer = new JacksonAvroSerializer<>();
		JacksonAvroDeserializer<ResponseMessage<PostingRequest, PostingResponse>> deserializer = new JacksonAvroDeserializer<>();
		JavaType type = serializer.getTypeFactory().constructParametricType(ResponseMessage.class, PostingRequest.class, PostingResponse.class);
		serializer.configure(config, false, type);
		deserializer.configure(config, false);
		return Serdes.serdeFrom(serializer, deserializer);
	}

	public TopologyTestDriver getTestDriver() {
		return testDriver;
	}

	public EnhancementTopology getEnhancementTopology() {
		return enhancementTopology;
	}

	public TestInputTopic<String, Account> getAccountTopic() {
		return accountTopic;
	}

	public TestInputTopic<String, TraceableMessage<PostingRequest>> getRequestTopic() {
		return requestTopic;
	}

	public TestOutputTopic<String, TraceableMessage<WorkflowMessage>> getEnhancedRequestTopic() {
		return enhancedRequestTopic;
	}

	public TestOutputTopic<String, ResponseMessage<PostingRequest, PostingResponse>> getResponseTopic() {
		return responseTopic;
	}

	public TestInputTopic<String, OverdraftInstruction> getOverdraftTopic() {
		return overdraftTopic;
	}

	public TestOutputTopic<UUID, TraceableMessage<WorkflowMessage>> getMatchReservationTopic() {
		return matchReservationTopic;
	}
}
