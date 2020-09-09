package qslv.kstream.enhancement;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.util.UUID;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestOutputTopic;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

import qslv.common.kafka.ResponseMessage;
import qslv.common.kafka.TraceableMessage;
import qslv.data.Account;
import qslv.data.OverdraftInstruction;
import qslv.kstream.CancelReservationRequest;
import qslv.kstream.PostingRequest;
import qslv.kstream.PostingResponse;
import qslv.kstream.workflow.CancelReservationWorkflow;
import qslv.kstream.workflow.WorkflowMessage;
import qslv.util.Random;

@ExtendWith(MockitoExtension.class)
class Unit_CancelTopology {

	public final static String AIT = "237482"; 
	public final static String TEST_TAXONOMY_ID = "9.9.9.9.9";
	public final static String CORRELATION_ID = UUID.randomUUID().toString();
	public final static String VALID_STATUS = "EF";
	public final static String INVALID_STATUS = "CL";
	public final static String JSON_DATA = "{\"value\": 234934}";

	static TestSetup context;
	
	@BeforeAll
	static void beforeAl() throws Exception {
		context = new TestSetup();
	}
	
	@AfterAll
	static void afterAll() {
		context.getTestDriver().close();
	}
	
	private void drain(TestOutputTopic<?, ?> topic) {
		while ( topic.getQueueSize() > 0) {
			topic.readKeyValue();
		}
	}

	@Test
	void test_cancel_success() {
    	drain(context.getResponseTopic());
    	drain(context.getMatchReservationTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupCancelRequest());
    	Account account = setupAccount( traceable.getPayload().getCancelReservationRequest().getAccountNumber(), true);
    	
    	// Execute
    	context.getAccountTopic().pipeInput(account.getAccountNumber(), account);
    	context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
    	KeyValue<UUID, TraceableMessage<WorkflowMessage>> keyvalue =
    			context.getMatchReservationTopic().readKeyValue();
    	
    	// -- verify -----
    	assertEquals(keyvalue.key, traceable.getPayload().getCancelReservationRequest().getReservationUuid());
    	assertNotNull(keyvalue.value);
    	verifyTraceData(traceable, keyvalue.value);
    	assertNotNull(keyvalue.value.getPayload());
    	
    	WorkflowMessage message = keyvalue.value.getPayload();
    	assertEquals( traceable.getPayload().getResponseKey(), message.getResponseKey());
    	assertTrue(message.hasCancelReservationWorkflow());
    	CancelReservationWorkflow workflow = message.getCancelReservationWorkflow();
    	verifyCancelRequest( traceable.getPayload().getCancelReservationRequest(), workflow.getRequest());
    	verifyAccount( workflow.getAccount(), account);
    	assertEquals(account.getAccountNumber(), workflow.getProcessingAccountNumber());
    	assertEquals(CancelReservationWorkflow.MATCH_RESERVATION, workflow.getState());
    	assertNull( workflow.getAccumulatedResults() );
    	assertNull( workflow.getErrorMessage() );
    	assertNull( workflow.getResults() );
	}
	
	@Test
	void test_cancel_fail_traceable() {
    	drain(context.getResponseTopic());
    	drain(context.getMatchReservationTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupCancelRequest());
    	Account account = setupAccount( traceable.getPayload().getCancelReservationRequest().getAccountNumber(), true);
    	traceable.setBusinessTaxonomyId(null);
    	
    	// Execute
    	context.getAccountTopic().pipeInput(account.getAccountNumber(), account);
    	context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
    	KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue =
    			context.getResponseTopic().readKeyValue();
    	
    	// -- verify -----
    	assertNotNull(keyvalue.value);
    	assertEquals(traceable.getPayload().getResponseKey(), keyvalue.key);
    	assertEquals(ResponseMessage.MALFORMED_MESSAGE, keyvalue.value.getStatus());
    	assertTrue( keyvalue.value.getErrorMessage().contains("Business Taxonomy Id Missing"));
    	verifyCancelRequest( traceable.getPayload().getCancelReservationRequest(), keyvalue.value.getRequest().getCancelReservationRequest());
	}
	
	@Test
	void test_cancel_fail_traceable_businesstaxonomy() {
    	drain(context.getResponseTopic());
    	drain(context.getMatchReservationTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupCancelRequest());
    	Account account = setupAccount( traceable.getPayload().getCancelReservationRequest().getAccountNumber(), true);
    	traceable.setBusinessTaxonomyId(null);
    	
    	// Execute
    	context.getAccountTopic().pipeInput(account.getAccountNumber(), account);
    	context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
    	KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue =
    			context.getResponseTopic().readKeyValue();
    	
    	// -- verify -----
    	assertNotNull(keyvalue.value);
    	assertEquals(traceable.getPayload().getResponseKey(), keyvalue.key);
    	assertEquals(ResponseMessage.MALFORMED_MESSAGE, keyvalue.value.getStatus());
    	assertTrue( keyvalue.value.getErrorMessage().contains("Business Taxonomy Id Missing"));
    	verifyCancelRequest( traceable.getPayload().getCancelReservationRequest(), keyvalue.value.getRequest().getCancelReservationRequest());
	}

	@Test
	void test_cancel_fail_traceable_setCorrelationId() {
    	drain(context.getResponseTopic());
    	drain(context.getMatchReservationTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupCancelRequest());
    	Account account = setupAccount( traceable.getPayload().getCancelReservationRequest().getAccountNumber(), true);
    	traceable.setCorrelationId(null);
    	
    	// Execute
    	context.getAccountTopic().pipeInput(account.getAccountNumber(), account);
    	context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
    	KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue =
    			context.getResponseTopic().readKeyValue();
    	
    	// -- verify -----
    	assertNotNull(keyvalue.value);
    	assertEquals(traceable.getPayload().getResponseKey(), keyvalue.key);
    	assertEquals(ResponseMessage.MALFORMED_MESSAGE, keyvalue.value.getStatus());
    	assertTrue( keyvalue.value.getErrorMessage().contains("Correlation Id Missing"));
    	verifyCancelRequest( traceable.getPayload().getCancelReservationRequest(), keyvalue.value.getRequest().getCancelReservationRequest());
	}

	@Test
	void test_cancel_fail_traceable_setMessageCreationTime() {
    	drain(context.getResponseTopic());
    	drain(context.getMatchReservationTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupCancelRequest());
    	Account account = setupAccount( traceable.getPayload().getCancelReservationRequest().getAccountNumber(), true);
    	traceable.setMessageCreationTime(null);
    	
    	// Execute
    	context.getAccountTopic().pipeInput(account.getAccountNumber(), account);
    	context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
    	KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue =
    			context.getResponseTopic().readKeyValue();
    	
    	// -- verify -----
    	assertNotNull(keyvalue.value);
    	assertEquals(traceable.getPayload().getResponseKey(), keyvalue.key);
    	assertEquals(ResponseMessage.MALFORMED_MESSAGE, keyvalue.value.getStatus());
    	assertTrue( keyvalue.value.getErrorMessage().contains("Message Creation Time Missing"));
    	verifyCancelRequest( traceable.getPayload().getCancelReservationRequest(), keyvalue.value.getRequest().getCancelReservationRequest());
	}

	@Test
	void test_cancel_fail_traceable_setAit() {
    	drain(context.getResponseTopic());
    	drain(context.getMatchReservationTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupCancelRequest());
    	Account account = setupAccount( traceable.getPayload().getCancelReservationRequest().getAccountNumber(), true);
    	traceable.setProducerAit(null);
    	
    	// Execute
    	context.getAccountTopic().pipeInput(account.getAccountNumber(), account);
    	context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
    	KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue =
    			context.getResponseTopic().readKeyValue();
    	
    	// -- verify -----
    	assertNotNull(keyvalue.value);
    	assertEquals(traceable.getPayload().getResponseKey(), keyvalue.key);
    	assertEquals(ResponseMessage.MALFORMED_MESSAGE, keyvalue.value.getStatus());
    	assertTrue( keyvalue.value.getErrorMessage().contains("Producer AIT Missing"));
    	verifyCancelRequest( traceable.getPayload().getCancelReservationRequest(), keyvalue.value.getRequest().getCancelReservationRequest());
	}
	

	@Test
	void test_cancel_fail_reservation_reservationId() {
		PostingRequest request = setupCancelRequest();
		request.getCancelReservationRequest().setReservationUuid(null);
		test_cancel_fail_reservation(request);
	}
	@Test
	void test_cancel_fail_reservation_requestId() {
		PostingRequest request = setupCancelRequest();
		request.getCancelReservationRequest().setRequestUuid(null);
		test_cancel_fail_reservation(request);
	}
	@Test
	void test_cancel_fail_reservation_json() {
		PostingRequest request = setupCancelRequest();
		request.getCancelReservationRequest().setJsonMetaData(null);
		test_cancel_fail_reservation(request);
	}
	
	void test_cancel_fail_reservation(PostingRequest request) {
    	drain(context.getResponseTopic());
    	drain(context.getMatchReservationTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( request);
    	Account account = setupAccount( traceable.getPayload().getCancelReservationRequest().getAccountNumber(), true);
    	
    	// Execute
    	context.getAccountTopic().pipeInput(account.getAccountNumber(), account);
    	context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
    	KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue =
    			context.getResponseTopic().readKeyValue();
    	
    	// -- verify -----
    	assertNotNull(keyvalue.value);
    	assertEquals(traceable.getPayload().getResponseKey(), keyvalue.key);
    	assertEquals(ResponseMessage.MALFORMED_MESSAGE, keyvalue.value.getStatus());
    	assertTrue( keyvalue.value.getErrorMessage().contains("Cancel Reservation requires"));
    	verifyCancelRequest( traceable.getPayload().getCancelReservationRequest(), keyvalue.value.getRequest().getCancelReservationRequest());
	}
	
	@Test
	void test_cancel_fail_reservation_accountNumber() {
    	drain(context.getResponseTopic());
    	drain(context.getMatchReservationTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupCancelRequest());
    	Account account = setupAccount( traceable.getPayload().getCancelReservationRequest().getAccountNumber(), true);
    	
    	// Execute
    	context.getAccountTopic().pipeInput(account.getAccountNumber(), account);
    	traceable.getPayload().getCancelReservationRequest().setAccountNumber(null);
    	context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
    	KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue =
    			context.getResponseTopic().readKeyValue();
    	
    	// -- verify -----
    	assertNotNull(keyvalue.value);
    	assertEquals(traceable.getPayload().getResponseKey(), keyvalue.key);
    	assertEquals(ResponseMessage.MALFORMED_MESSAGE, keyvalue.value.getStatus());
    	assertTrue( keyvalue.value.getErrorMessage().contains("Cancel Reservation requires"));
    	verifyCancelRequest( traceable.getPayload().getCancelReservationRequest(), keyvalue.value.getRequest().getCancelReservationRequest());
	}
	
	@Test
	void test_cancel_success_with_overdraft() {
    	drain(context.getResponseTopic());
    	drain(context.getMatchReservationTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupCancelRequest());
    	Account account = setupAccount( traceable.getPayload().getCancelReservationRequest().getAccountNumber(), true);
		OverdraftInstruction instruction = setupOverdraft(account.getAccountNumber(), -10L, 10L, true, true);
    	
    	// Execute
		context.getOverdraftTopic().pipeInput(account.getAccountNumber(), instruction);
    	context.getAccountTopic().pipeInput(account.getAccountNumber(), account);
    	context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
    	KeyValue<UUID, TraceableMessage<WorkflowMessage>> keyvalue =
    			context.getMatchReservationTopic().readKeyValue();
    	
    	// -- verify -----
    	assertEquals(keyvalue.key, traceable.getPayload().getCancelReservationRequest().getReservationUuid());
    	assertNotNull(keyvalue.value);
    	verifyTraceData(traceable, keyvalue.value);
    	assertNotNull(keyvalue.value.getPayload());
    	
    	WorkflowMessage message = keyvalue.value.getPayload();
    	assertEquals( traceable.getPayload().getResponseKey(), message.getResponseKey());
    	assertTrue(message.hasCancelReservationWorkflow());
    	CancelReservationWorkflow workflow = message.getCancelReservationWorkflow();
    	verifyCancelRequest( traceable.getPayload().getCancelReservationRequest(), workflow.getRequest());
    	verifyAccount( workflow.getAccount(), account);
    	assertEquals(account.getAccountNumber(), workflow.getProcessingAccountNumber());
    	assertEquals(CancelReservationWorkflow.MATCH_RESERVATION, workflow.getState());
    	assertNull( workflow.getAccumulatedResults() );
    	assertNull( workflow.getErrorMessage() );
    	assertNull( workflow.getResults() );
	}
	
	private void verifyAccount(Account expected, Account actual) {
		assertEquals(expected.getAccountLifeCycleStatus(), actual.getAccountLifeCycleStatus());		
		assertEquals(expected.getAccountNumber(), actual.getAccountNumber());
	}

	Account setupAccount(String accountNumber, boolean effective) {
		Account account = new Account();
		account.setAccountNumber(accountNumber);
		account.setAccountLifeCycleStatus(effective ? "EF" : "CL");
		return account;
	}
	
	private OverdraftInstruction setupOverdraft(String accountNumber, long startDays, long endDays, boolean effective, boolean accountOpen) {
		OverdraftInstruction instruction = new OverdraftInstruction();
		instruction.setAccountNumber(accountNumber);
		instruction.setEffectiveStart(LocalDateTime.now().plusDays(startDays));
		instruction.setEffectiveEnd(LocalDateTime.now().plusDays(endDays));
		instruction.setInstructionLifecycleStatus(effective ? "EF" : "CL");
		instruction.setOverdraftAccount(setupAccount(Random.randomDigits(12), accountOpen));
		return instruction;
	}

	PostingRequest setupCancelRequest() {
		CancelReservationRequest request = new CancelReservationRequest();
		request.setAccountNumber(Random.randomDigits(12));
		request.setJsonMetaData(JSON_DATA);
		request.setRequestUuid(UUID.randomUUID());
		request.setReservationUuid(UUID.randomUUID());
		return new PostingRequest(request, Random.randomAlphaNum(15));
	}
	
	TraceableMessage<PostingRequest> setupRequest(PostingRequest request) {
		TraceableMessage<PostingRequest> trequest = new TraceableMessage<>();
		trequest.setBusinessTaxonomyId(TEST_TAXONOMY_ID);
		trequest.setCorrelationId(CORRELATION_ID);
		trequest.setMessageCreationTime(LocalDateTime.now());
		trequest.setPayload(request);
		trequest.setProducerAit(AIT);
		return trequest;
	}

	private void verifyTraceData(TraceableMessage<?> expected, TraceableMessage<?> actual) {
		assertEquals(expected.getBusinessTaxonomyId(), actual.getBusinessTaxonomyId());
		assertEquals(expected.getCorrelationId(), actual.getCorrelationId());
		assertEquals(expected.getMessageCreationTime(), actual.getMessageCreationTime());
		assertEquals(expected.getProducerAit(), actual.getProducerAit());
		assertEquals(expected.getMessageCompletionTime(), actual.getMessageCompletionTime());
	}
	private void verifyCancelRequest(CancelReservationRequest expected, CancelReservationRequest actual) {
		assertEquals( expected.getAccountNumber(), actual.getAccountNumber());
		assertEquals( expected.getRequestUuid(), actual.getRequestUuid());
		assertEquals( expected.getJsonMetaData(), actual.getJsonMetaData());
		assertEquals( expected.getReservationUuid(), actual.getReservationUuid());
	}

}
