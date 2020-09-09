package qslv.kstream.enhancement;

import static org.junit.jupiter.api.Assertions.*;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
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
import qslv.kstream.TransactionRequest;
import qslv.kstream.workflow.TransactionWorkflow;
import qslv.kstream.workflow.WorkflowMessage;
import qslv.util.Random;

@ExtendWith(MockitoExtension.class)
class Unit_TransactionTopology {

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
	void test_transaction_success_positive() {
		test_transaction_success(1L);
	}
	@Test
	void test_transaction_success_negative() {
		test_transaction_success(-1L);
	}
	void test_transaction_success(long transactionAmount) {
    	drain(context.getResponseTopic());
    	drain(context.getEnhancedRequestTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupTransactionRequest(transactionAmount));
    	Account account = setupAccount( traceable.getPayload().getTransactionRequest().getAccountNumber(), true);
    	
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
    	assertTrue(message.hasTransactionWorkflow());
    	TransactionWorkflow workflow = message.getTransactionWorkflow();
    	verifyTransactionRequest( traceable.getPayload().getTransactionRequest(), workflow.getRequest());
    	verifyAccount( workflow.getAccount(), account);
    	assertEquals(account.getAccountNumber(), workflow.getProcessingAccountNumber());
    	assertEquals(TransactionWorkflow.TRANSACT_START, workflow.getState());
    	assertNull( workflow.getAccumulatedResults() );
    	assertNull( workflow.getErrorMessage() );
    	assertNull( workflow.getProcessedInstructions() );
    	assertNull( workflow.getResults() );
    	assertNull( workflow.getUnprocessedInstructions() );
	}
	
	public interface SetupOverdraft {
		void setup(String accountNumber);
	}
	public interface VerifyOverdraft {
		void verify(List<OverdraftInstruction> actual);
	}

	void test_transaction_with_overdrafts(SetupOverdraft setupOverdraft, VerifyOverdraft verifyOverdraft) {
    	drain(context.getResponseTopic());
    	drain(context.getEnhancedRequestTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupTransactionRequest(-1L));
    	Account account = setupAccount( traceable.getPayload().getTransactionRequest().getAccountNumber(), true);
    	setupOverdraft.setup(account.getAccountNumber());
    	
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
    	assertTrue(message.hasTransactionWorkflow());
    	TransactionWorkflow workflow = message.getTransactionWorkflow();
    	verifyTransactionRequest( traceable.getPayload().getTransactionRequest(), workflow.getRequest());
    	verifyAccount( workflow.getAccount(), account);
    	assertEquals(account.getAccountNumber(), workflow.getProcessingAccountNumber());
    	assertEquals(TransactionWorkflow.TRANSACT_START, workflow.getState());
    	assertNull( workflow.getAccumulatedResults() );
    	assertNull( workflow.getErrorMessage() );
    	assertNull( workflow.getProcessedInstructions() );
    	assertNull( workflow.getResults() );
    	
    	verifyOverdraft.verify(workflow.getUnprocessedInstructions());
	}
	
	@Test
	void test_transaction_OD_one() {
		test_transaction_with_overdrafts((a) -> setupOverdraft_one_valid(a), (a) -> verifyOverdraft_all_valid(a));
	}

	ArrayList<OverdraftInstruction> expected;
	void setupOverdraft_one_valid(String accountNumber) {
		expected = new ArrayList<>();
		OverdraftInstruction instruction = setupOverdraft(accountNumber, -10L, 10L, true, true);
		expected.add(instruction);
		context.getOverdraftTopic().pipeInput(accountNumber, instruction);
	}
	
	void verifyOverdraft_all_valid(List<OverdraftInstruction> actual) {
		assertEquals(expected.size(), actual.size());
		for (int ii = 0; ii < 0; ii++ ) {
			verifyOverdraft(expected.get(ii), actual.get(ii));
		}
	}
	
	@Test
	void test_transaction_three_OD() {
		test_transaction_with_overdrafts((a) -> setupOverdraft_three_valid(a), (a) -> verifyOverdraft_all_valid(a));
	}

	void setupOverdraft_three_valid(String accountNumber) {
		expected = new ArrayList<>();
		for (long ii = 0; ii < 3; ii++) {
			OverdraftInstruction instruction = setupOverdraft(accountNumber, (-10L-ii), (10L+ii), true, true);
			expected.add(instruction);
			context.getOverdraftTopic().pipeInput(accountNumber, instruction);
		}
	}
	
	@Test
	void test_transaction_invalid_OD() {
		test_transaction_with_overdrafts((a) -> setupOverdraft_invalid(a), (a) -> verifyOverdraft_all_valid(a));
	}

	void setupOverdraft_invalid(String accountNumber) {
		expected = new ArrayList<>();
		
		// one valid
		OverdraftInstruction instruction = setupOverdraft(accountNumber, (-10L), (10L), true, true);
		expected.add(instruction);
		context.getOverdraftTopic().pipeInput(accountNumber, instruction);
		
		// various invalid
		instruction = setupOverdraft(accountNumber, (10L), (10L), true, true);
		context.getOverdraftTopic().pipeInput(accountNumber, instruction);
		instruction = setupOverdraft(accountNumber, (-10L), (-10L), true, true);
		context.getOverdraftTopic().pipeInput(accountNumber, instruction);
		instruction = setupOverdraft(accountNumber, (-10L), (10L), false, true);
		context.getOverdraftTopic().pipeInput(accountNumber, instruction);
		instruction = setupOverdraft(accountNumber, (-10L), (10L), true, false);
		context.getOverdraftTopic().pipeInput(accountNumber, instruction);

		// one more valid
		instruction = setupOverdraft(accountNumber, (-10L), (10L), true, true);
		expected.add(instruction);
		context.getOverdraftTopic().pipeInput(accountNumber, instruction);
	}
	
	@Test
	void test_transaction_fail_traceable() {
    	drain(context.getResponseTopic());
    	drain(context.getEnhancedRequestTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupTransactionRequest(-1L));
    	Account account = setupAccount( traceable.getPayload().getTransactionRequest().getAccountNumber(), true);
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
    	verifyTransactionRequest( traceable.getPayload().getTransactionRequest(), keyvalue.value.getRequest().getTransactionRequest());
	}
	
	@Test
	void test_transaction_fail_traceable_businesstaxonomy() {
    	drain(context.getResponseTopic());
    	drain(context.getEnhancedRequestTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupTransactionRequest(-1L));
    	Account account = setupAccount( traceable.getPayload().getTransactionRequest().getAccountNumber(), true);
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
    	verifyTransactionRequest( traceable.getPayload().getTransactionRequest(), keyvalue.value.getRequest().getTransactionRequest());
	}

	@Test
	void test_transaction_fail_traceable_setCorrelationId() {
    	drain(context.getResponseTopic());
    	drain(context.getEnhancedRequestTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupTransactionRequest(-1L));
    	Account account = setupAccount( traceable.getPayload().getTransactionRequest().getAccountNumber(), true);
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
    	verifyTransactionRequest( traceable.getPayload().getTransactionRequest(), keyvalue.value.getRequest().getTransactionRequest());
	}

	@Test
	void test_transaction_fail_traceable_setMessageCreationTime() {
    	drain(context.getResponseTopic());
    	drain(context.getEnhancedRequestTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupTransactionRequest(-1L));
    	Account account = setupAccount( traceable.getPayload().getTransactionRequest().getAccountNumber(), true);
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
    	verifyTransactionRequest( traceable.getPayload().getTransactionRequest(), keyvalue.value.getRequest().getTransactionRequest());
	}

	@Test
	void test_transaction_fail_traceable_setAit() {
    	drain(context.getResponseTopic());
    	drain(context.getEnhancedRequestTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupTransactionRequest(-1L));
    	Account account = setupAccount( traceable.getPayload().getTransactionRequest().getAccountNumber(), true);
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
    	verifyTransactionRequest( traceable.getPayload().getTransactionRequest(), keyvalue.value.getRequest().getTransactionRequest());
	}
	
	@Test
	void test_transaction_fail_reservation_zeroAmount() {
		PostingRequest request = setupTransactionRequest(0L);
		test_transaction_fail_reservation(request);
	}
	@Test
	void test_transaction_fail_reservation_requestId() {
		PostingRequest request = setupTransactionRequest(-1L);
		request.getTransactionRequest().setRequestUuid(null);
		test_transaction_fail_reservation(request);
	}
	@Test
	void test_transaction_fail_reservation_json() {
		PostingRequest request = setupTransactionRequest(-1L);
		request.getTransactionRequest().setJsonMetaData(null);
		test_transaction_fail_reservation(request);
	}
	
	void test_transaction_fail_reservation(PostingRequest request) {
    	drain(context.getResponseTopic());
    	drain(context.getEnhancedRequestTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( request);
    	Account account = setupAccount( traceable.getPayload().getTransactionRequest().getAccountNumber(), true);
    	
    	// Execute
    	context.getAccountTopic().pipeInput(account.getAccountNumber(), account);
    	context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
    	KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue =
    			context.getResponseTopic().readKeyValue();
    	
    	// -- verify -----
    	assertNotNull(keyvalue.value);
    	assertEquals(traceable.getPayload().getResponseKey(), keyvalue.key);
    	assertEquals(ResponseMessage.MALFORMED_MESSAGE, keyvalue.value.getStatus());
    	assertTrue( keyvalue.value.getErrorMessage().contains("Transaction requires"));
    	verifyTransactionRequest( traceable.getPayload().getTransactionRequest(), keyvalue.value.getRequest().getTransactionRequest());
	}
	
	@Test
	void test_transaction_fail_reservation_accountNumber() {
    	drain(context.getResponseTopic());
    	drain(context.getEnhancedRequestTopic());

    	// Setup 
    	TraceableMessage<PostingRequest> traceable = setupRequest( setupTransactionRequest(-1L));
    	Account account = setupAccount( traceable.getPayload().getTransactionRequest().getAccountNumber(), true);
    	
    	// Execute
    	context.getAccountTopic().pipeInput(account.getAccountNumber(), account);
    	traceable.getPayload().getTransactionRequest().setAccountNumber(null);
    	context.getRequestTopic().pipeInput(account.getAccountNumber(), traceable);
    	KeyValue<String, ResponseMessage<PostingRequest, PostingResponse>> keyvalue =
    			context.getResponseTopic().readKeyValue();
    	
    	// -- verify -----
    	assertNotNull(keyvalue.value);
    	assertEquals(traceable.getPayload().getResponseKey(), keyvalue.key);
    	assertEquals(ResponseMessage.MALFORMED_MESSAGE, keyvalue.value.getStatus());
    	assertTrue( keyvalue.value.getErrorMessage().contains("Transaction requires"));
    	verifyTransactionRequest( traceable.getPayload().getTransactionRequest(), keyvalue.value.getRequest().getTransactionRequest());
	}

	private void verifyOverdraft(OverdraftInstruction expected, OverdraftInstruction actual) {
		assertEquals(expected.getAccountNumber(), actual.getAccountNumber());
		assertEquals(expected.getEffectiveEnd(), actual.getEffectiveEnd());
		assertEquals(expected.getEffectiveStart(), actual.getEffectiveStart());
		assertEquals(expected.getInstructionLifecycleStatus(), actual.getInstructionLifecycleStatus());
		assertEquals(expected.getOverdraftAccount().getAccountNumber(), actual.getOverdraftAccount().getAccountNumber());
		assertEquals(expected.getOverdraftAccount().getAccountLifeCycleStatus(), actual.getOverdraftAccount().getAccountLifeCycleStatus());
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
	PostingRequest setupTransactionRequest(long transactionAmount) {
		TransactionRequest request = new TransactionRequest();
		request.setAccountNumber(Random.randomDigits(12));
		request.setDebitCardNumber(Random.randomDigits(16));
		request.setJsonMetaData(JSON_DATA);
		request.setRequestUuid(UUID.randomUUID());
		request.setTransactionAmount(transactionAmount);
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
	private void verifyTransactionRequest(TransactionRequest expected, TransactionRequest actual) {
		assertEquals( expected.getAccountNumber(), actual.getAccountNumber());
		assertEquals( expected.getDebitCardNumber(), actual.getDebitCardNumber());
		assertEquals( expected.getRequestUuid(), actual.getRequestUuid());
		assertEquals( expected.getJsonMetaData(), actual.getJsonMetaData());
		assertEquals( expected.getTransactionAmount(), actual.getTransactionAmount());
	}

}
