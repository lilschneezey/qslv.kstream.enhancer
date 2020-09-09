package qslv.kstream.enhancement;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import qslv.util.EnableQuickSilver;

@Configuration
@ConfigurationProperties(prefix = "qslv")
@EnableQuickSilver
public class ConfigProperties {

	private String aitid = "27834";
	
	private String postingRequestTopic = null;
	private String enhancedRequestTopic = null;
	private String postingResponseTopic = null;
	private String accountLogTopic = null;
	private String overdraftLogTopic = null;
	private String matchReservationTopic = null;
	private String kafkaConsumerPropertiesPath = null;
	private String kafkaProducerPropertiesPath = null;
	private String kafkaStreamsPropertiesPath = null;
	
	public String getAitid() {
		return aitid;
	}
	public void setAitid(String aitid) {
		this.aitid = aitid;
	}
	public String getEnhancedRequestTopic() {
		return enhancedRequestTopic;
	}
	public void setEnhancedRequestTopic(String enhancedRequestTopic) {
		this.enhancedRequestTopic = enhancedRequestTopic;
	}
	public String getPostingResponseTopic() {
		return postingResponseTopic;
	}
	public void setPostingResponseTopic(String postingResponseTopic) {
		this.postingResponseTopic = postingResponseTopic;
	}
	public String getKafkaConsumerPropertiesPath() {
		return kafkaConsumerPropertiesPath;
	}
	public void setKafkaConsumerPropertiesPath(String kafkaConsumerPropertiesPath) {
		this.kafkaConsumerPropertiesPath = kafkaConsumerPropertiesPath;
	}
	public String getKafkaProducerPropertiesPath() {
		return kafkaProducerPropertiesPath;
	}
	public void setKafkaProducerPropertiesPath(String kafkaProducerPropertiesPath) {
		this.kafkaProducerPropertiesPath = kafkaProducerPropertiesPath;
	}
	public String getKafkaStreamsPropertiesPath() {
		return kafkaStreamsPropertiesPath;
	}
	public void setKafkaStreamsPropertiesPath(String kafkaStreamsPropertiesPath) {
		this.kafkaStreamsPropertiesPath = kafkaStreamsPropertiesPath;
	}
	public String getPostingRequestTopic() {
		return postingRequestTopic;
	}
	public void setPostingRequestTopic(String postingRequestTopic) {
		this.postingRequestTopic = postingRequestTopic;
	}
	public String getAccountLogTopic() {
		return accountLogTopic;
	}
	public void setAccountLogTopic(String accountLogTopic) {
		this.accountLogTopic = accountLogTopic;
	}
	public String getOverdraftLogTopic() {
		return overdraftLogTopic;
	}
	public void setOverdraftLogTopic(String overdraftLogTopic) {
		this.overdraftLogTopic = overdraftLogTopic;
	}
	public String getMatchReservationTopic() {
		return matchReservationTopic;
	}
	public void setMatchReservationTopic(String matchReservationTopic) {
		this.matchReservationTopic = matchReservationTopic;
	}

}
