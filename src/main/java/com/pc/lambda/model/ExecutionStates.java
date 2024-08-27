package com.pc.lambda.model;

import lombok.Data;

@Data
public class ExecutionStates {
	
	private boolean documentFound;
	private boolean fileDownloaded;
	private boolean processVerified;
	private boolean reportSent;
	private boolean documentUpdated;
}
