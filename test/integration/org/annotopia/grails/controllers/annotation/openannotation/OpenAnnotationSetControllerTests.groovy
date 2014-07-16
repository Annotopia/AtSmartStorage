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
package org.annotopia.grails.controllers.annotation.openannotation;

import static org.junit.Assert.*
import grails.test.mixin.TestFor

import org.annotopia.grails.controllers.annotation.deprecated.openannotation.set.OpenAnnotationSetController

/**
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
@TestFor(OpenAnnotationSetController)
class OpenAnnotationSetControllerTests extends GroovyTestCase {

	def grailsApplication = new org.codehaus.groovy.grails.commons.DefaultGrailsApplication()
	
	void testShowInvalidApiKey() {
		def c = new OpenAnnotationSetController()
		c.show()
		
		assertEquals 401, response.status
	}
	
	void testShowValidApiKey() {
		def c = new OpenAnnotationController()
		c.request.JSON = '{"apiKey":"' + grailsApplication.config.annotopia.storage.testing.apiKey + '"}'
		c.show()
		
		assertEquals 200, response.status
	}
	
//	void testValidationInvalidApiKey() {
//		def c = new OpenAnnotationSetController()
//		c.validate()
//		
//		assertEquals 401, response.status
//	}
}
