package org.refractions.cabd;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * Custom configuration parameters that appear in the application.properties file
 * @author Emily
 *
 */
@Component
@ConfigurationProperties("cabd")
public class CabdConfigurationProperties {

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
