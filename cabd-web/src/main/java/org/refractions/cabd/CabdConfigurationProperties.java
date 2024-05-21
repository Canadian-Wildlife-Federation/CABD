/*
 * Copyright 2021 Canadian Wildlife Federation
 * 
 * Licensed under the Apache License, Version 2.0 (the "License"); 
 * you may not use this file except in compliance with the License. 
 * You may obtain a copy of the License at
 * 
 * http://www.apache.org/licenses/LICENSE-2.0
 * 
 * Unless required by applicable law or agreed to in writing, software 
 * distributed under the License is distributed on an "AS IS" BASIS, 
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. 
 * See the License for the specific language governing permissions and 
 * limitations under the License.
 */
package org.refractions.cabd;

import java.util.Map;

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
	
	private long vectorcachesize = 0;
	private long cachefree = 0;
	
	private Map<String, String> azureSettings;
	
	/**
	 * The storage azure account url 
	 * @return
	 */
	public String getAzureStorageaccounturl() {
		return azureSettings.get("storageaccounturl");
	}

	/**
	 * The azure container name
	 * @return
	 */
	public String getAzureContainername() {
		return azureSettings.get("containername");
	}


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
	
	/**
	 * The maximum size of vector tiles in the cache table
	 * @return
	 */
	public long getVectorcachesize() {
		return this.vectorcachesize;
	}
	
	public void setVectorcachesize(long vectorcachesize) {
		this.vectorcachesize = vectorcachesize;
	}
	
	/**
	 * The amount of space that should be free in the
	 * cache after cleaning out old items
	 * @return
	 */
	public long getCachefree() {
		return this.cachefree;
	}
	
	public void setCachefree(long cachefree) {
		this.cachefree = cachefree;
	}

	public Map<String, String> getAzure() {
		return azureSettings;
	}

	public void setAzure(Map<String, String> azureSettings) {
		this.azureSettings = azureSettings;
	}
}
