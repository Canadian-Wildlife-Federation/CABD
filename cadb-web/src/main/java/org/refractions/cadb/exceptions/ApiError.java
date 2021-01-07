package org.refractions.cadb.exceptions;

import java.time.LocalDateTime;

import com.fasterxml.jackson.annotation.JsonFormat;

/**
 * Generic error field contains a date/time and message.
 * @author Emily
 *
 */
public class ApiError {

	@JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "dd-MM-yyyy hh:mm:ss")
	private LocalDateTime timestamp;
	private String message;

	private ApiError() {
		timestamp = LocalDateTime.now();
	}

	ApiError(Throwable ex) {
		this();
		this.message = ex.getMessage();
	}

	ApiError(String message) {
		this();
		this.message = message;
	}
	
	public String getMessage() {
		return this.message;
	}

}
