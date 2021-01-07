package org.refractions.cadb;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Custom configuration parameters that appear in the application.properties file
 * @author Emily
 *
 */
@Component
@ConfigurationProperties("cadb")
public class CadbConfigurationProperties {

	private int maxresults = 10000;
	
	/**
	 * The system defined maximum number of search results
	 * @return
	 */
	public int getMaxresults() {
		return this.maxresults;
	}
	
	public void setMaxresults(int maxresults) {
		this.maxresults = maxresults;
	}
}
