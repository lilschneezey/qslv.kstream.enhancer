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
import qslv.kstream.PostingRequest;
import qslv.kstream.PostingResponse;
import qslv.kstream.TransferRequest;
import qslv.kstream.workflow.TransferWorkflow;
import qslv.kstream.workflow.WorkflowMessage;
import qslv.util.Random;

@ExtendWith(MockitoExtension.class)
class Unit_TransferTopology {

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
	void test_reservation_success() {
    	drain(context.getResponseTopic());
    	drain(context.getEnhancedRequestTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupTransferRequest(1L));
    	Account account = setupAccount( traceable.getPayload().getTransferRequest().getTransferFromAccountNumber(), true);
    	
    	// Execute
    	context.getAccountTopic().pipeInput(account.getAccountNumber(), account);
    	context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
    	KeyValue<String, TraceableMessage<WorkflowMessage>> keyvalue =
    			context.getEnhancedRequestTopic().readKeyValue();
    	
    	// -- verify -----
    	assertEquals(keyvalue.key, account.getAccountNumber());
    	assertNotNull(keyvalue.value);
    	verifyTraceData(traceable, keyvalue.value);
    	assertNotNull(keyvalue.value.getPayload());
    	
    	WorkflowMessage message = keyvalue.value.getPayload();
    	assertEquals( traceable.getPayload().getResponseKey(), message.getResponseKey());
    	assertTrue(message.hasTransferWorkflow());
    	TransferWorkflow workflow = message.getTransferWorkflow();
    	verifyTransferRequest( traceable.getPayload().getTransferRequest(), workflow.getRequest());
    	verifyAccount( workflow.getTransferFromAccount(), account);
    	assertEquals(account.getAccountNumber(), workflow.getProcessingAccountNumber());
    	assertEquals(TransferWorkflow.START_TRANSFER_FUNDS, workflow.getState());
    	assertNull( workflow.getAccumulatedResults() );
    	assertNull( workflow.getErrorMessage() );
    	assertNull( workflow.getResults() );
	}
	
	@Test
	void test_reservation_success_withoverdraft() {
    	drain(context.getResponseTopic());
    	drain(context.getEnhancedRequestTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupTransferRequest(1L));
    	Account account = setupAccount( traceable.getPayload().getTransferRequest().getTransferFromAccountNumber(), true);
		OverdraftInstruction instruction = setupOverdraft(account.getAccountNumber(), -10L, 10L, true, true);

    	// Execute
		context.getOverdraftTopic().pipeInput(account.getAccountNumber(), instruction);
    	context.getAccountTopic().pipeInput(account.getAccountNumber(), account);
    	context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
    	KeyValue<String, TraceableMessage<WorkflowMessage>> keyvalue =
    			context.getEnhancedRequestTopic().readKeyValue();
    	
    	// -- verify -----
    	assertEquals(keyvalue.key, account.getAccountNumber());
    	assertNotNull(keyvalue.value);
    	verifyTraceData(traceable, keyvalue.value);
    	assertNotNull(keyvalue.value.getPayload());
    	
    	WorkflowMessage message = keyvalue.value.getPayload();
    	assertEquals( traceable.getPayload().getResponseKey(), message.getResponseKey());
    	assertTrue(message.hasTransferWorkflow());
    	TransferWorkflow workflow = message.getTransferWorkflow();
    	verifyTransferRequest( traceable.getPayload().getTransferRequest(), workflow.getRequest());
    	verifyAccount( workflow.getTransferFromAccount(), account);
    	assertEquals(account.getAccountNumber(), workflow.getProcessingAccountNumber());
    	assertEquals(TransferWorkflow.START_TRANSFER_FUNDS, workflow.getState());
    	assertNull( workflow.getAccumulatedResults() );
    	assertNull( workflow.getErrorMessage() );
    	assertNull( workflow.getResults() );
	}
	
	@Test
	void test_reservation_fail_traceable() {
    	drain(context.getResponseTopic());
    	drain(context.getEnhancedRequestTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupTransferRequest(-1L));
    	Account account = setupAccount( traceable.getPayload().getTransferRequest().getTransferFromAccountNumber(), true);
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
    	verifyTransferRequest( traceable.getPayload().getTransferRequest(), keyvalue.value.getRequest().getTransferRequest());
	}
	
	@Test
	void test_reservation_fail_traceable_businesstaxonomy() {
    	drain(context.getResponseTopic());
    	drain(context.getEnhancedRequestTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupTransferRequest(-1L));
    	Account account = setupAccount( traceable.getPayload().getTransferRequest().getTransferFromAccountNumber(), true);
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
    	verifyTransferRequest( traceable.getPayload().getTransferRequest(), keyvalue.value.getRequest().getTransferRequest());
	}

	@Test
	void test_reservation_fail_traceable_setCorrelationId() {
    	drain(context.getResponseTopic());
    	drain(context.getEnhancedRequestTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupTransferRequest(-1L));
    	Account account = setupAccount( traceable.getPayload().getTransferRequest().getTransferFromAccountNumber(), true);
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
    	verifyTransferRequest( traceable.getPayload().getTransferRequest(), keyvalue.value.getRequest().getTransferRequest());
	}

	@Test
	void test_reservation_fail_traceable_setMessageCreationTime() {
    	drain(context.getResponseTopic());
    	drain(context.getEnhancedRequestTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupTransferRequest(-1L));
    	Account account = setupAccount( traceable.getPayload().getTransferRequest().getTransferFromAccountNumber(), true);
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
    	verifyTransferRequest( traceable.getPayload().getTransferRequest(), keyvalue.value.getRequest().getTransferRequest());
	}

	@Test
	void test_reservation_fail_traceable_setAit() {
    	drain(context.getResponseTopic());
    	drain(context.getEnhancedRequestTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupTransferRequest(-1L));
    	Account account = setupAccount( traceable.getPayload().getTransferRequest().getTransferFromAccountNumber(), true);
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
    	verifyTransferRequest( traceable.getPayload().getTransferRequest(), keyvalue.value.getRequest().getTransferRequest());
	}
	
	@Test
	void test_reservation_fail_reservation_zeroAmount() {
		PostingRequest request = setupTransferRequest(0L);
		test_reservation_fail_reservation(request);
	}
	@Test
	void test_reservation_fail_reservation_toAccountNumber() {
		PostingRequest request = setupTransferRequest(1L);
		request.getTransferRequest().setTransferToAccountNumber(null);
		test_reservation_fail_reservation(request);
	}
	@Test
	void test_reservation_fail_reservation_requestId() {
		PostingRequest request = setupTransferRequest(-1L);
		request.getTransferRequest().setRequestUuid(null);
		test_reservation_fail_reservation(request);
	}
	@Test
	void test_reservation_fail_reservation_json() {
		PostingRequest request = setupTransferRequest(-1L);
		request.getTransferRequest().setJsonMetaData(null);
		test_reservation_fail_reservation(request);
	}
	
	void test_reservation_fail_reservation(PostingRequest request) {
    	drain(context.getResponseTopic());
    	drain(context.getEnhancedRequestTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( request);
    	Account account = setupAccount( traceable.getPayload().getTransferRequest().getTransferFromAccountNumber(), true);
    	
    	// Execute
    	context.getAccountTopic().pipeInput(account.getAccountNumber(), account);
    	context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
    	KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue =
    			context.getResponseTopic().readKeyValue();
    	
    	// -- verify -----
    	assertNotNull(keyvalue.value);
    	assertEquals(traceable.getPayload().getResponseKey(), keyvalue.key);
    	assertEquals(ResponseMessage.MALFORMED_MESSAGE, keyvalue.value.getStatus());
    	assertTrue( keyvalue.value.getErrorMessage().contains("Transfer requires"));
    	verifyTransferRequest( traceable.getPayload().getTransferRequest(), keyvalue.value.getRequest().getTransferRequest());
	}
	
	@Test
	void test_reservation_fail_reservation_accountNumber() {
    	drain(context.getResponseTopic());
    	drain(context.getEnhancedRequestTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupTransferRequest(-1L));
    	Account account = setupAccount( traceable.getPayload().getTransferRequest().getTransferFromAccountNumber(), true);
    	
    	// Execute
    	context.getAccountTopic().pipeInput(account.getAccountNumber(), account);
    	traceable.getPayload().getTransferRequest().setTransferFromAccountNumber(null);
    	context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
    	KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue =
    			context.getResponseTopic().readKeyValue();
    	
    	// -- verify -----
    	assertNotNull(keyvalue.value);
    	assertEquals(traceable.getPayload().getResponseKey(), keyvalue.key);
    	assertEquals(ResponseMessage.MALFORMED_MESSAGE, keyvalue.value.getStatus());
    	assertTrue( keyvalue.value.getErrorMessage().contains("Transfer requires"));
    	verifyTransferRequest( traceable.getPayload().getTransferRequest(), keyvalue.value.getRequest().getTransferRequest());
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
	PostingRequest setupTransferRequest(long transactionAmount) {
		TransferRequest request = new TransferRequest();
		request.setTransferFromAccountNumber(Random.randomDigits(12));
		request.setTransferToAccountNumber(Random.randomDigits(12));
		request.setJsonMetaData(JSON_DATA);
		request.setRequestUuid(UUID.randomUUID());
		request.setTransferAmount(transactionAmount);
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
	private void verifyTransferRequest(TransferRequest expected, TransferRequest actual) {
		assertEquals( expected.getTransferFromAccountNumber(), actual.getTransferFromAccountNumber());
		assertEquals( expected.getTransferToAccountNumber(), actual.getTransferToAccountNumber());
		assertEquals( expected.getRequestUuid(), actual.getRequestUuid());
		assertEquals( expected.getJsonMetaData(), actual.getJsonMetaData());
		assertEquals( expected.getTransferAmount(), actual.getTransferAmount());
	}

}
