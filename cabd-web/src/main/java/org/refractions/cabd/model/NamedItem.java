/*
 * Copyright 2022 Canadian Wildlife Federation
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
package org.refractions.cabd.model;

import org.refractions.cabd.CabdApplication;

/**
 * Simple object containing english and french name fields that
 * returns the correct name based on the request locale.
 * 
 * @author Emily
 *
 */
public class NamedItem {

	protected String name_en;
	protected String name_fr;
	
	public NamedItem(String name_en, String name_fr) {
		this.name_en = name_en;
		this.name_fr = name_fr;
	}
	
	
	public String getName() {
		if (CabdApplication.isFrench())  return name_fr;
		return name_en;	
	}
}
