/*
 * Copyright 2014 Massachusetts General Hospital
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.annotopia.grails.services.storage.utils.jena

import java.text.SimpleDateFormat

import org.annotopia.grails.vocabularies.Bibliographic
import org.annotopia.grails.vocabularies.RDF

import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory
import com.hp.hpl.jena.rdf.model.ResourceFactory
import com.hp.hpl.jena.rdf.model.StmtIterator


/**
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class GraphIdentifiersMetadataService {

	def grailsApplication
	
	SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss.SSSz")
	
	public Model getIdentifiersGraphMetadata(String apiKey, Dataset dataset, def graphUri, Map<String, String> identifiers) {
		//def metaGraphUri = getGraphUri();
		def graphRes = ResourceFactory.createResource(graphUri);
		Model metaModel;
		if(dataset.getNamedModel(grailsApplication.config.annotopia.storage.uri.graph.identifiers)!=null) {
			 metaModel = dataset.getNamedModel(grailsApplication.config.annotopia.storage.uri.graph.identifiers);
		} else {
			 metaModel = ModelFactory.createDefaultModel();
			 dataset.addNamedModel(grailsApplication.config.annotopia.storage.uri.graph.identifiers, metaModel);
		}
		
		if(identifiers.get(Bibliographic.LABEL_URL)!=null) {
			metaModel.add(ResourceFactory.createResource(identifiers.get(Bibliographic.LABEL_URL)),
				ResourceFactory.createProperty(Bibliographic.EMBODIMENT_OF), graphRes);
			
			metaModel.add(ResourceFactory.createResource(identifiers.get(Bibliographic.LABEL_URL)),
				ResourceFactory.createProperty(RDF.RDF_TYPE), ResourceFactory.createResource(Bibliographic.WEB_PAGE));
			
			metaModel.add(graphRes, ResourceFactory.createProperty(RDF.RDF_TYPE), 
				ResourceFactory.createResource(Bibliographic.EXPRESSION));
			
			
			if(identifiers.get(Bibliographic.LABEL_DOI)!=null) {
				metaModel.add(graphRes, ResourceFactory.createProperty(Bibliographic.DOI),
					ResourceFactory.createPlainLiteral(identifiers.get(Bibliographic.LABEL_DOI)));
			}
			
			if(identifiers.get(Bibliographic.LABEL_PMID)!=null) {
				metaModel.add(graphRes, ResourceFactory.createProperty(Bibliographic.PMID),
					ResourceFactory.createPlainLiteral(identifiers.get(Bibliographic.LABEL_PMID)));
			}
			
			if(identifiers.get(Bibliographic.LABEL_PMCID)!=null) {
				metaModel.add(graphRes, ResourceFactory.createProperty(Bibliographic.PMCID),
					ResourceFactory.createPlainLiteral(identifiers.get(Bibliographic.LABEL_PMCID)));
			}
			
			if(identifiers.get(Bibliographic.LABEL_PII)!=null) {
				metaModel.add(graphRes, ResourceFactory.createProperty(Bibliographic.PII),
					ResourceFactory.createPlainLiteral(identifiers.get(Bibliographic.LABEL_PII)));
			}
			
			dataset.addNamedModel(grailsApplication.config.annotopia.storage.uri.graph.identifiers, metaModel);
		}
		metaModel
	}
	
	public Model updateIdentifiersGraphMetadata(String apiKey, Model metaModel, Map<String, String> identifiers) {
		
		StmtIterator wrapper = metaModel.listStatements(null, ResourceFactory.createProperty(Bibliographic.EMBODIMENT_OF), null);
		if(wrapper.hasNext()) {
			def object = wrapper.next().getObject();
			StmtIterator identifiersData = metaModel.listStatements(object, null, null);
			while(identifiersData.hasNext()) {
				println identifiersData.next()
			}
		}
		
		
		
		
		
//		if(identifiers.get(Bibliographic.LABEL_PMID)!=null) {
//			metaModel.add(graphRes, ResourceFactory.createProperty(Bibliographic.PMID),
//				ResourceFactory.createPlainLiteral(identifiers.get(Bibliographic.LABEL_PMID)));
//		}
	}
}
