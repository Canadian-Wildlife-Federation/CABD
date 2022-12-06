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
package org.refractions.cabd.serializers;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

import javax.xml.transform.OutputKeys;
import javax.xml.transform.Result;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.sax.SAXTransformerFactory;
import javax.xml.transform.sax.TransformerHandler;
import javax.xml.transform.stream.StreamResult;

import org.apache.commons.lang3.tuple.ImmutableTriple;
import org.geotools.data.collection.ListFeatureCollection;
import org.geotools.feature.simple.SimpleFeatureBuilder;
import org.geotools.kml.v22.KML;
import org.geotools.xsd.Encoder;
import org.locationtech.jts.geom.Envelope;
import org.opengis.feature.simple.SimpleFeature;
import org.opengis.feature.simple.SimpleFeatureType;
import org.refractions.cabd.CabdApplication;
import org.refractions.cabd.dao.FeatureTypeManager;
import org.refractions.cabd.model.Feature;
import org.refractions.cabd.model.FeatureList;
import org.refractions.cabd.model.FeatureType;
import org.refractions.cabd.model.FeatureViewMetadata;
import org.refractions.cabd.model.FeatureViewMetadataField;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpOutputMessage;
import org.springframework.http.converter.HttpMessageNotWritableException;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.support.ServletUriComponentsBuilder;
import org.xml.sax.Attributes;
import org.xml.sax.Locator;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.AttributesImpl;

/**
 * Serializes a list of Feature to kml dataset
 * 
 * @author Emily
 *
 */
@Component
public class FeatureListKmlSerializer extends AbstractFeatureListSerializer{

	@Autowired
	private FeatureTypeManager typeManager;
		
	public FeatureListKmlSerializer() {
		super(CabdApplication.KML_MEDIA_TYPE);
	}

	@Override
	protected void writeInternal(FeatureList features, HttpOutputMessage outputMessage)
			throws IOException, HttpMessageNotWritableException {

		super.writeInternal(features, outputMessage);

		if (features.getItems().isEmpty())
			return;

		ImmutableTriple<String, FeatureViewMetadata, Envelope> metadataitems = FeatureListUtil.getMetadata(features,
				typeManager);

		FeatureViewMetadata metadata = metadataitems.getMiddle();

		SimpleFeatureType type = FeatureListUtil.asFeatureType(metadataitems.getLeft(), metadata);

		ListFeatureCollection cfeatures = new ListFeatureCollection(type);

		SimpleFeatureBuilder builder = new SimpleFeatureBuilder(type);

		String rooturl = ServletUriComponentsBuilder.fromCurrentContextPath().path("/").build().toUriString();
		for (Feature f : features.getItems()) {
			for (FeatureViewMetadataField field : metadata.getFields()) {
				if (field.isGeometry()) {
					builder.set(field.getFieldName(), f.getGeometry());
				}
				if (f.getAttributes().containsKey(field.getFieldName())) {
					Object value = f.getAttribute(field.getFieldName());
					builder.set(field.getFieldName(), value);
				} else if (f.getLinkAttributes().containsKey(field.getFieldName())) {
					String value = f.getLinkAttributes().get(field.getFieldName());
					if (value != null) {
						builder.set(field.getFieldName(), rooturl + value);
					} else {
						builder.set(field.getFieldName(), null);
					}
				}
			}
			SimpleFeature sf = builder.buildFeature(f.getId().toString());
			cfeatures.add(sf);
		}

		outputMessage.getHeaders().set(HttpHeaders.CONTENT_DISPOSITION, FeatureListUtil.getContentDispositionHeader(metadataitems.getLeft(), "kml"));
		outputMessage.getHeaders().set(HttpHeaders.CONTENT_TYPE, CabdApplication.KML_MEDIA_TYPE.getType());

		Map<String, String> metadataItems = new HashMap<>();
		metadataItems.put(FeatureListUtil.DATA_LICENSE_KEY, CabdApplication.DATA_LICENCE_URL);
		metadataItems.put(FeatureListUtil.DOWNLOAD_DATETIME_KEY, FeatureListUtil.getNowAsString());		
		for (String sftype : FeatureListUtil.getFeatureTypes(features)) {
			FeatureType ft = typeManager.getFeatureType(sftype);
			metadataItems.put(ft.getType() + "_" + FeatureListUtil.DATA_VERSION_KEY, ft.getDataVersion());
		}

		Encoder encoder = new Encoder(new org.geotools.kml.v22.KMLConfiguration());
		encoder.setNamespaceAware(false);

		SAXTransformerFactory txFactory = (SAXTransformerFactory) SAXTransformerFactory.newInstance();
		TransformerHandler xmls = null;
		try {
			xmls = new TransformerHandlerWrapper(txFactory.newTransformerHandler(), metadataItems);
		} catch (TransformerConfigurationException e) {
			throw new IOException(e);
		}
		xmls.getTransformer().setOutputProperty(OutputKeys.METHOD, "xml");
		xmls.setResult(new StreamResult(outputMessage.getBody()));
		try {
			encoder.encode(cfeatures, org.geotools.kml.v22.KML.kml, xmls);
		} catch (IOException e) {
			throw e;
		} catch (SAXException e) {
			throw new IOException(e);
		}

	}
	
	/**
	 * Hack to add metadata to document element as extended data. The only way 
	 * I could figure this out was to make a wrapper around the transformerHandler.
	 * 
	 *
	 */
	class TransformerHandlerWrapper implements TransformerHandler{

		private TransformerHandler wrapper;
		private Map<String, String> metadataItems;
		
		public TransformerHandlerWrapper(TransformerHandler wrapper, Map<String, String> metadataItems) {
			this.wrapper = wrapper;
			this.metadataItems = metadataItems;
		}
		
		@Override
		public void setDocumentLocator(Locator locator) {
			wrapper.setDocumentLocator(locator);
			
		}

		@Override
		public void startDocument() throws SAXException {
			wrapper.startDocument();
		}

		@Override
		public void endDocument() throws SAXException {
			wrapper.endDocument();			
		}

		@Override
		public void startPrefixMapping(String prefix, String uri) throws SAXException {
			wrapper.startPrefixMapping(prefix, uri);
			
		}

		@Override
		public void endPrefixMapping(String prefix) throws SAXException {
			wrapper.endPrefixMapping(prefix);
			
		}

		@Override
		public void startElement(String uri, String localName, String qName, Attributes atts) throws SAXException {
			wrapper.startElement(uri, localName, qName, atts);
			
	        if (KML.Document.getLocalPart().equals(localName)) {

	        	startElement(uri, KML.ExtendedData.getLocalPart(), KML.ExtendedData.getLocalPart(), new AttributesImpl());
	        	
	        	for (Entry<String,String> key : metadataItems.entrySet()) {
		        	AttributesImpl names = new AttributesImpl();
		        	names.addAttribute(uri, KML.name.getLocalPart(), KML.name.getLocalPart(), "String", key.getKey());
		        	startElement(uri, KML.Data.getLocalPart(), KML.Data.getLocalPart(), names);
		        	startElement(uri, KML.value.getLocalPart(), KML.value.getLocalPart(), new AttributesImpl());
		        	if (key.getKey().equals(FeatureListUtil.DATA_LICENSE_KEY)) {
			        	startCDATA();
			        	characters(key.getValue().toCharArray(), 0, key.getValue().length());
			        	endCDATA();
		        	}else {
		        		characters(key.getValue().toCharArray(), 0, key.getValue().length());
		        	}
		        	endElement(uri, KML.value.getLocalPart(), KML.value.getLocalPart());
		        	endElement(uri, KML.Data.getLocalPart(), KML.Data.getLocalPart());
	        	}
	        	
	        	endElement(uri, KML.ExtendedData.getLocalPart(), KML.ExtendedData.getLocalPart());
	        }
		}

		@Override
		public void endElement(String uri, String localName, String qName) throws SAXException {
			wrapper.endElement(uri, localName, qName);
			
		}

		@Override
		public void characters(char[] ch, int start, int length) throws SAXException {
			wrapper.characters(ch, start, length);
			
		}

		@Override
		public void ignorableWhitespace(char[] ch, int start, int length) throws SAXException {
			wrapper.ignorableWhitespace(ch, start, length);
			
		}

		@Override
		public void processingInstruction(String target, String data) throws SAXException {
			wrapper.processingInstruction(target, data);
			
		}

		@Override
		public void skippedEntity(String name) throws SAXException {
			wrapper.skippedEntity(name);
			
		}

		@Override
		public void startDTD(String name, String publicId, String systemId) throws SAXException {
			wrapper.startDTD(name, publicId, systemId);
			
		}

		@Override
		public void endDTD() throws SAXException {
			wrapper.endDTD();
			
		}

		@Override
		public void startEntity(String name) throws SAXException {
			wrapper.startEntity(name);
			
		}

		@Override
		public void endEntity(String name) throws SAXException {
			wrapper.endEntity(name);
		}

		@Override
		public void startCDATA() throws SAXException {
			wrapper.startCDATA();
		}

		@Override
		public void endCDATA() throws SAXException {
			wrapper.endCDATA();
		}

		@Override
		public void comment(char[] ch, int start, int length) throws SAXException {
			wrapper.comment(ch, start, length);
		}

		@Override
		public void notationDecl(String name, String publicId, String systemId) throws SAXException {
			wrapper.notationDecl(name, publicId, systemId);
		}

		@Override
		public void unparsedEntityDecl(String name, String publicId, String systemId, String notationName)
				throws SAXException {
			wrapper.unparsedEntityDecl(name, publicId, systemId, notationName);
		}

		@Override
		public void setResult(Result result) throws IllegalArgumentException {
			wrapper.setResult(result);
		}

		@Override
		public void setSystemId(String systemID) {
			wrapper.setSystemId(systemID);
		}

		@Override
		public String getSystemId() {
			return wrapper.getSystemId();
		}

		@Override
		public Transformer getTransformer() {
			return wrapper.getTransformer();
		}
		
	}
}
