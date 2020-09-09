package qslv.kstream.enhancement;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.UUID;

import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import qslv.common.kafka.ResponseMessage;
import qslv.common.kafka.TraceableMessage;
import qslv.data.Account;
import qslv.data.OverdraftInstruction;
import qslv.data.OverdraftInstructions;
import qslv.kstream.BaseTransactionRequest;
import qslv.kstream.CancelReservationRequest;
import qslv.kstream.CommitReservationRequest;
import qslv.kstream.PostingResponse;
import qslv.kstream.PostingRequest;
import qslv.kstream.ReservationRequest;
import qslv.kstream.TransactionRequest;
import qslv.kstream.TransferRequest;
import qslv.kstream.workflow.CancelReservationWorkflow;
import qslv.kstream.workflow.CommitReservationWorkflow;
import qslv.kstream.workflow.ReservationWorkflow;
import qslv.kstream.workflow.TransactionWorkflow;
import qslv.kstream.workflow.TransferWorkflow;
import qslv.kstream.workflow.Workflow;
import qslv.kstream.workflow.WorkflowMessage;

@Component
public class EnhancementTopology {
	private static final Logger log = LoggerFactory.getLogger(EnhancementTopology.class);

	@Autowired
	ConfigProperties configProperties;
	@Autowired
	Serde<Account> accountSerde;
	@Autowired
	Serde<OverdraftInstruction> overdraftSerde;
	@Autowired
	Serde<OverdraftInstructions> overdraftCollectionSerde;
	@Autowired
	Serde<TraceableMessage<PostingRequest>> postingRequestSerde;
	@Autowired
	Serde<TraceableMessage<WorkflowMessage>> enhancedPostingRequestSerde;
	@Autowired
	Serde<ResponseMessage<PostingRequest, PostingResponse>> postingReplySerde;

	public void setConfigProperties(ConfigProperties configProperties) {
		this.configProperties = configProperties;
	}

	public void setAccountSerde(Serde<Account> accountSerde) {
		this.accountSerde = accountSerde;
	}

	public void setOverdraftSerde(Serde<OverdraftInstruction> overdraftSerde) {
		this.overdraftSerde = overdraftSerde;
	}

	public void setOverdraftCollectionSerde(Serde<OverdraftInstructions> overdraftCollectionSerde) {
		this.overdraftCollectionSerde = overdraftCollectionSerde;
	}

	public void setPostingRequestSerde(Serde<TraceableMessage<PostingRequest>> postingRequestSerde) {
		this.postingRequestSerde = postingRequestSerde;
	}

	public void setEnhancedPostingRequestSerde(Serde<TraceableMessage<WorkflowMessage>> enhancedPostingRequestSerde) {
		this.enhancedPostingRequestSerde = enhancedPostingRequestSerde;
	}

	public void setPostingReplySerde(Serde<ResponseMessage<PostingRequest, PostingResponse>> postingReplySerde) {
		this.postingReplySerde = postingReplySerde;
	}

	@Bean
	public KStream<?, ?> kStream(StreamsBuilder builder) {
		log.debug("kStream ENTRY");
		KStream<String, TraceableMessage<PostingRequest>> rawRequests = builder.stream(configProperties.getPostingRequestTopic(),
				Consumed.with(Serdes.String(), postingRequestSerde));
		KTable<String, Account> accounts = builder.table(configProperties.getAccountLogTopic(), 
				Consumed.with(Serdes.String(), accountSerde));
		KTable<String, OverdraftInstructions> overdraftInstructions = builder.stream(configProperties.getOverdraftLogTopic(),
				Consumed.with(Serdes.String(), overdraftSerde))
				.groupByKey()
				.aggregate(() -> new OverdraftInstructions(new ArrayList<OverdraftInstruction>()),
						(k,v,a) -> {a.getOverdrafts().add(v); return a; },
						Materialized.with(Serdes.String(), overdraftCollectionSerde) );
		
		@SuppressWarnings("unchecked")
		KStream<String, TraceableMessage<WorkflowMessage>>[] accountStatusBranches = rawRequests
				.peek((k,v) -> log.debug("request recieved {}", k))
				.mapValues((v) -> mapWorkflowMessage(v))
				.leftJoin(accounts, (s,t) -> joinAccount(s,t) )
				.branch(
						(k,v) -> (isValidMessage(k,v)), 
						(k,v) -> (true) 
				);
		
		//-- return the failures back to the requester
		accountStatusBranches[1]
				.map((k,v) -> mapResponse(k,v))
				.to(configProperties.getPostingResponseTopic(), Produced.with(Serdes.String(), postingReplySerde));

		//-- continue with overdraft instructions
		@SuppressWarnings("unchecked")
		KStream<String, TraceableMessage<WorkflowMessage>>[] outputBranches = 
				accountStatusBranches[0]
				.leftJoin(overdraftInstructions, (s,t) -> joinOverdraft(s,t))
				.branch(
						(k,v) -> (needsReservation(k,v)),
						(k,v) -> (true)
				);
		outputBranches[0]
				.map((k,v) -> rekeyReservationUuid(v))
				.to(configProperties.getMatchReservationTopic(), Produced.with(Serdes.UUID(), enhancedPostingRequestSerde));
		outputBranches[1].to(configProperties.getEnhancedRequestTopic(), Produced.with(Serdes.String(), enhancedPostingRequestSerde));

		log.debug("kStream EXIT");
		return rawRequests;
	}
	
	boolean needsReservation(String key, TraceableMessage<WorkflowMessage> traceable) {
		return traceable.getPayload().hasCancelReservationWorkflow() || 
				traceable.getPayload().hasCommitReservationWorkflow();
	}
	
	KeyValue<UUID, TraceableMessage<WorkflowMessage>> rekeyReservationUuid( TraceableMessage<WorkflowMessage> traceableMessage ) {
		UUID key;
		if (traceableMessage.getPayload().hasCancelReservationWorkflow())
			key = traceableMessage.getPayload().getCancelReservationWorkflow().getRequest().getReservationUuid();

		else if (traceableMessage.getPayload().hasCommitReservationWorkflow())
			key = traceableMessage.getPayload().getCommitReservationWorkflow().getRequest().getReservationUuid();
		
		else key = UUID.randomUUID(); // ??
		
		return new KeyValue<>(key, traceableMessage);
		
}

	TraceableMessage<WorkflowMessage> mapWorkflowMessage( TraceableMessage<PostingRequest> traceableRequest ) {
		log.debug("mapWorkflowMessage ENTRY");
		PostingRequest request = traceableRequest.getPayload();
		Workflow workflow = null;
		if ( request.hasCancelReservationRequest() ) {
			workflow = new CancelReservationWorkflow(CancelReservationWorkflow.MATCH_RESERVATION, request.getCancelReservationRequest());
			workflow.setProcessingAccountNumber(request.getCancelReservationRequest().getAccountNumber());
			
		} else if ( request.hasCommitReservationRequest() ) {
			workflow = new CommitReservationWorkflow(CommitReservationWorkflow.MATCH_RESERVATION, request.getCommitReservationRequest());
			workflow.setProcessingAccountNumber(request.getCommitReservationRequest().getAccountNumber());
			
		} else if ( request.hasReservationRequest() ) {
			workflow = new ReservationWorkflow(ReservationWorkflow.ACQUIRE_RESERVATION, request.getReservationRequest());
			workflow.setProcessingAccountNumber(request.getReservationRequest().getAccountNumber());
			
		} else if ( request.hasTransferRequest() ) {
			workflow = new TransferWorkflow(TransferWorkflow.START_TRANSFER_FUNDS, request.getTransferRequest());
			workflow.setProcessingAccountNumber(request.getTransferRequest().getTransferFromAccountNumber());
			
		} else if ( request.hasTransactionRequest() ){
			workflow = new TransactionWorkflow(TransactionWorkflow.TRANSACT_START, request.getTransactionRequest());
			workflow.setProcessingAccountNumber(request.getTransactionRequest().getAccountNumber());
		} else {
			workflow = null;
			log.error("Request not a valid type.");
		}
		return new TraceableMessage<>(traceableRequest, new WorkflowMessage(workflow, request.getResponseKey()));
	}
	
	TraceableMessage<WorkflowMessage> joinAccount( TraceableMessage<WorkflowMessage> traceableRequest, Account account ) {
		log.debug("joinAccount ENTRY.  Account {} found.", account==null ? "not" : account.getAccountNumber());
		WorkflowMessage message = traceableRequest.getPayload();
		if ( message.hasCancelReservationWorkflow() ) {
			message.getCancelReservationWorkflow().setAccount(account);
		} else if ( message.hasCommitReservationWorkflow() ) {
			message.getCommitReservationWorkflow().setAccount(account);
		} else if ( message.hasReservationWorkflow() ) {
			message.getReservationWorkflow().setAccount(account);
		} else if ( message.hasTransferWorkflow() ) {
			message.getTransferWorkflow().setTransferFromAccount(account);
		} else if ( message.hasTransactionWorkflow() ){
			message.getTransactionWorkflow().setAccount(account);
		}
		return traceableRequest;
	}

	TraceableMessage<WorkflowMessage> joinOverdraft(TraceableMessage<WorkflowMessage> traceableRequest, OverdraftInstructions overdrafts) {
		log.debug("joinOverdraft ENTRY, OD size=={}", overdrafts==null ? "null" : overdrafts.getOverdrafts().size() );

		if (overdrafts == null ) {
			log.debug("Overdrafts null.");
			return traceableRequest;
		}

		WorkflowMessage message = traceableRequest.getPayload();
		ArrayList<OverdraftInstruction> unprocessed = null;
		if (message.hasCommitReservationWorkflow()) {
			if (message.getCommitReservationWorkflow().getUnprocessedInstructions() == null)
				message.getCommitReservationWorkflow().setUnprocessedInstructions(new ArrayList<>());
			unprocessed = message.getCommitReservationWorkflow().getUnprocessedInstructions();
		
		} else if ( message.hasReservationWorkflow() ) {
			if (message.getReservationWorkflow().getUnprocessedInstructions() == null)
				message.getReservationWorkflow().setUnprocessedInstructions(new ArrayList<>());
			unprocessed = message.getReservationWorkflow().getUnprocessedInstructions();
		
		} else if ( message.hasTransactionWorkflow() ) {
			if (message.getTransactionWorkflow().getUnprocessedInstructions() == null)
				message.getTransactionWorkflow().setUnprocessedInstructions(new ArrayList<>());
			unprocessed = message.getTransactionWorkflow().getUnprocessedInstructions();
		
		} else {
			return traceableRequest;
		}

		for (OverdraftInstruction overdraft : overdrafts.getOverdrafts()) {
			if (isValidOverdraft(overdraft)) {
				log.debug("joinOverdraft valid");
				unprocessed.add(overdraft);
			} else {
				log.debug("joinOverdraft invalid {} {} {}", overdraft.getEffectiveStart(), overdraft.getEffectiveEnd(),
						overdraft.getInstructionLifecycleStatus());
			}
		}

		return traceableRequest;
	}
	
	boolean isValidOverdraft(OverdraftInstruction overdraft ) {
		return overdraft.getInstructionLifecycleStatus().equals("EF")
			&& (overdraft.getEffectiveStart() == null || overdraft.getEffectiveStart().isBefore(LocalDateTime.now()))
			&& (overdraft.getEffectiveEnd() == null || overdraft.getEffectiveEnd().isAfter(LocalDateTime.now()))
			&& overdraft.getOverdraftAccount() != null && isAccountLifecycleStatusValid(overdraft.getOverdraftAccount());
	}
	
	boolean isValidMessage(String key, TraceableMessage<WorkflowMessage> traceableRequest) {
		if ( ! hasValidTraceData(traceableRequest) ) {
			log.debug("Status {} Message {}", traceableRequest.getPayload().getWorkflow().getState(), 
					traceableRequest.getPayload().getWorkflow().getErrorMessage());
			return false;
		}
		if ( ! hasValidRequest(traceableRequest.getPayload()) ) {
			log.debug("Status {} Message {}", traceableRequest.getPayload().getWorkflow().getState(), 
					traceableRequest.getPayload().getWorkflow().getErrorMessage());
			return false;
		}
		if ( ! hasValidAccount(traceableRequest.getPayload()) ) {
			log.debug("Status {} Message {}", traceableRequest.getPayload().getWorkflow().getState(), 
					traceableRequest.getPayload().getWorkflow().getErrorMessage());
			return false;
		}
		log.debug("Request valid for key {}", key);
		return true;
	}
	boolean hasValidTraceData( TraceableMessage<WorkflowMessage> message) {
		if (message.getBusinessTaxonomyId() == null) {
			message.getPayload().getWorkflow().setState(Workflow.VALIDATION_FAILURE);
			message.getPayload().getWorkflow().setErrorMessage("Business Taxonomy Id Missing.");
			return false;
		}
		if (message.getCorrelationId() == null) {
			message.getPayload().getWorkflow().setState(Workflow.VALIDATION_FAILURE);
			message.getPayload().getWorkflow().setErrorMessage("Correlation Id Missing.");
			return false;
		}
		if (message.getMessageCreationTime() == null) {
			message.getPayload().getWorkflow().setState(Workflow.VALIDATION_FAILURE);
			message.getPayload().getWorkflow().setErrorMessage("Message Creation Time Missing.");
			return false;
		}
		if (message.getProducerAit() == null) {
			message.getPayload().getWorkflow().setState(Workflow.VALIDATION_FAILURE);
			message.getPayload().getWorkflow().setErrorMessage("Producer AIT Missing.");
			return false;
		}
		return true;
	}
	boolean hasValidRequest( WorkflowMessage message ) {
		if ( message.hasCancelReservationWorkflow() && !isValid(message.getCancelReservationWorkflow().getRequest())) {
			message.getWorkflow().setState(Workflow.VALIDATION_FAILURE);
			message.getWorkflow().setErrorMessage("Cancel Reservation requires Account Number, Request Id, Reservation UUID, and Json Meta Data.");
			return false;
		}
		if ( message.hasCommitReservationWorkflow() && !isValid(message.getCommitReservationWorkflow().getRequest())) {
			message.getWorkflow().setState(Workflow.VALIDATION_FAILURE);
			message.getWorkflow().setErrorMessage("Commit Reservation requires Account Number, Request Id, Reservation UUID, Non-Zero Amount, and Json Meta Data.");
			return false;
		}
		if ( message.hasReservationWorkflow() && !isValid(message.getReservationWorkflow().getRequest())) {
			message.getWorkflow().setState(Workflow.VALIDATION_FAILURE);
			message.getWorkflow().setErrorMessage("Reservation requires Account Number, Request Id, Non-Zero Amount, and Json Meta Data.");
			return false;
		}
		if ( message.hasTransactionWorkflow() && !isValid(message.getTransactionWorkflow().getRequest())) {
			message.getWorkflow().setState(Workflow.VALIDATION_FAILURE);
			message.getWorkflow().setErrorMessage("Transaction requires Account Number, Request Id, Non-Zero Amount, and Json Meta Data.");
			return false;
		}
		if ( message.hasTransferWorkflow() && !isValid(message.getTransferWorkflow().getRequest())) {
			message.getWorkflow().setState(Workflow.VALIDATION_FAILURE);
			message.getWorkflow().setErrorMessage("Transfer requires From Account Number, To Account Number, Request Id, Non-Zero Amount, and Json Meta Data.");
			return false;
		}
		return true;
	}
	boolean isValid(CancelReservationRequest request) {
		if (request.getAccountNumber() == null) return false;
		if (request.getRequestUuid() == null) return false;
		if (request.getReservationUuid() == null) return false;
		if (request.getJsonMetaData() == null) return false;
		return true;
	}
	boolean isValid(CommitReservationRequest request) {
		if (request.getAccountNumber() == null) return false;
		if (request.getRequestUuid() == null) return false;
		if (request.getJsonMetaData() == null) return false;
		if (request.getReservationUuid() == null) return false;
		if (request.getTransactionAmount() >= 0L) return false;
		return true;
	}
	boolean isValid(ReservationRequest request) {
		if (request.getAccountNumber() == null) return false;
		if (request.getRequestUuid() == null) return false;
		if (request.getJsonMetaData() == null) return false;
		if (request.getTransactionAmount() >= 0L) return false;
		return true;
	}
	boolean isValid(TransactionRequest request) {
		if (request.getAccountNumber() == null) return false;
		if (request.getRequestUuid() == null) return false;
		if (request.getJsonMetaData() == null) return false;
		if (request.getTransactionAmount() == 0L) return false;
		return true;
	}
	boolean isValid(TransferRequest request) {
		if (request.getTransferFromAccountNumber() == null) return false;
		if (request.getTransferToAccountNumber() == null) return false;
		if (request.getRequestUuid() == null) return false;
		if (request.getJsonMetaData() == null) return false;
		if (request.getTransferAmount() <= 0L) return false;
		return true;
	}
	boolean hasValidAccount(WorkflowMessage message) {
		if ( message.hasCancelReservationWorkflow()) {
			return isValidAccount(message, message.getCancelReservationWorkflow().getAccount());
		} else if (message.hasCommitReservationWorkflow()) {
			return isValidAccount(message, message.getCommitReservationWorkflow().getAccount());
		} else if ( message.hasReservationWorkflow()) {
			return isValidAccount(message, message.getReservationWorkflow().getAccount());
		} else if ( message.hasTransactionWorkflow()) {
			return isValidAccount(message, message.getTransactionWorkflow().getAccount());
		}else if ( message.hasTransferWorkflow()) {
			return isValidAccount(message, message.getTransferWorkflow().getTransferFromAccount());
		}
		log.error("Unknown message state. hasValidAccount()");
		return false;
	}

	boolean isValidAccount( WorkflowMessage message, Account account ) {
		if ( account == null )
			return false;
		if ( ! isAccountLifecycleStatusValid(account) ) {
			message.getWorkflow().setErrorMessage(String.format("Invalid Account Status {} {}", 
					account.getAccountNumber(), account.getAccountLifeCycleStatus() ));
			message.getWorkflow().setState(Workflow.CONFLICT_FAILURE);
			return false;
		}
		return true;		
	}
	
	boolean isAccountLifecycleStatusValid( Account account ) {
		return account != null && account.getAccountLifeCycleStatus().equals("EF");
	}
	
	KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> mapResponse(String key, TraceableMessage<WorkflowMessage> traceable) {
		String errorMessage = traceable.getPayload().getWorkflow().getErrorMessage();
		int state = traceable.getPayload().getWorkflow().getState();
		log.debug("Responding with Status {} Message {}", state, errorMessage==null ? "null" : errorMessage);
		
		int status;
		switch( state ) {
		case Workflow.CONFLICT_FAILURE:
			status = ResponseMessage.CONFLICT; 
			break;
		case Workflow.VALIDATION_FAILURE:
			status = ResponseMessage.MALFORMED_MESSAGE; 
			break;
		default:
			status = ResponseMessage.INTERNAL_ERROR;
		}

		BaseTransactionRequest request = null;
		if ( traceable.getPayload().hasCancelReservationWorkflow()) {
			request = traceable.getPayload().getCancelReservationWorkflow().getRequest();
		} else if (traceable.getPayload().hasCommitReservationWorkflow()) {
			request = traceable.getPayload().getCommitReservationWorkflow().getRequest();
		} else if ( traceable.getPayload().hasReservationWorkflow()) {
			request = traceable.getPayload().getReservationWorkflow().getRequest();
		} else if ( traceable.getPayload().hasTransactionWorkflow()) {
			request = traceable.getPayload().getTransactionWorkflow().getRequest();
		}else if ( traceable.getPayload().hasTransferWorkflow()) {
			request = traceable.getPayload().getTransferWorkflow().getRequest();
		}
		ResponseMessage<PostingRequest, PostingResponse> response = new ResponseMessage<>(traceable, new PostingRequest(request), null);
		response.setMessageCompletionTime(LocalDateTime.now());
		response.setStatus(status);
		response.setErrorMessage(errorMessage);
		return new KeyValue<>(traceable.getPayload().getResponseKey(), response);
	}
}
