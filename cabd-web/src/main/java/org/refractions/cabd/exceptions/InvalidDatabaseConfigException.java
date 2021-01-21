
package org.refractions.cabd.exceptions;

public class InvalidDatabaseConfigException extends RuntimeException {
	
	private static final long serialVersionUID = 1L;
	
	private ApiError errorMessage;
	
	public InvalidDatabaseConfigException(String message) {
		super(message);
		errorMessage = new ApiError(message);
	}
	
	public ApiError getError() {
		return errorMessage;
	}
	
}
