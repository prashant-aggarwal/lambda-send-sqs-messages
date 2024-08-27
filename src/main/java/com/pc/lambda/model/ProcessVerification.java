package com.pc.lambda.model;

import lombok.Data;

@Data
public class ProcessVerification {
	private boolean formatData;
	private boolean matchData;
	private boolean validateData;
	private boolean matchSource;
	private boolean formatExcel;
	private boolean calcuateExcel;
}
