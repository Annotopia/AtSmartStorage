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
package org.annotopia.grails.vocabularies;

/**
 * This is a class collecting the properties defined by the Open
 * Annotation Model (see: http://www.openannotation.org/spec/core/) 
 * 
 * @author Paolo Ciccarese <paolo.ciccarese@gmail.com>
 */
public interface OA {

	public static final String ANNOTATION 			= "http://www.w3.org/ns/oa#Annotation";
	public static final String SPECIFIC_RESOURCE    = "http://www.w3.org/ns/oa#SpecificResource";
	public static final String CONTEXT_AS_TEXT      = "http://www.w3.org/2011/content#ContentAsText";
	public static final String GRAPH      			="http://www.w3.org/2004/03/trix/rdfg-1/Graph";
	
	public static final String HAS_BODY    			= "http://www.w3.org/ns/oa#hasBody";
}
