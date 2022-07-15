package org.refractions.cabd.model;

import org.refractions.cabd.CabdApplication;

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
