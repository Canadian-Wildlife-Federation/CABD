package org.refractions.cabd.model;

public class CrsInfo {

	private String auth;
	private int authCode;
	private int pgSrid;
	
	public CrsInfo(String auth, int authCode, int pgSrid) {
		this.auth = auth;
		this.authCode = authCode;
		this.pgSrid = pgSrid;
	}
	
	public Integer getSrid() {
		return this.pgSrid;
	}
	
	public String getCrsString() {
		return this.auth + ":" + this.authCode;
	}
}
