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
package org.annotopia.grails.services.storage

/**
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
class CleanUpService {

	def annotationIntegratedStorageService
	def openAnnotationVirtuosoService
	def jenaVirtuosoStoreService
	def configAccessService
	
	def clearAllGraphs = {
		Set<String> annotationSetsGraphNames = annotationIntegratedStorageService.retrieveAnnotationSetsGraphsNames("secret", 100, 0, null, "true");
		annotationSetsGraphNames.each { graph ->
			jenaVirtuosoStoreService.dropGraph("secret", graph);
		}
		
		Set<String> annotationGraphNames = openAnnotationVirtuosoService.retrieveAnnotationGraphsNames("secret", 100, 0, null, "true", null, null);
		annotationGraphNames.each { graph ->
			jenaVirtuosoStoreService.dropGraph("secret", graph);
		}
		
		jenaVirtuosoStoreService.dropGraph("secret", configAccessService.getAsString("annotopia.storage.uri.graph.provenance"));
		jenaVirtuosoStoreService.dropGraph("secret", configAccessService.getAsString("annotopia.storage.uri.graph.identifiers"));
	}
}
