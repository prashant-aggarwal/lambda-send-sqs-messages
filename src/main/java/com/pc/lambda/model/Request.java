package com.pc.lambda.model;

import lombok.Data;

@Data
public class Request {
	
	private int recordId;
	private int priority;
	private boolean sendReport;
	private ExecutionStates executionStates;
	private ProcessVerification processVerification;
}
