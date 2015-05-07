package org.annotopia.grails.controllers.openannotationset

import groovy.lang.Closure;

import java.util.Set;
import java.util.regex.Matcher

import org.annotopia.grails.vocabularies.AnnotopiaVocabulary
import org.annotopia.grails.vocabularies.PAV
import org.annotopia.groovy.service.store.BaseController
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.RDFLanguages

import com.hp.hpl.jena.query.Dataset
import com.hp.hpl.jena.query.DatasetFactory
import com.hp.hpl.jena.rdf.model.Model
import com.hp.hpl.jena.rdf.model.ModelFactory;
import com.hp.hpl.jena.rdf.model.Resource;
import com.hp.hpl.jena.rdf.model.ResourceFactory
/**
 * @author Finn Bacall <finn.bacall@manchester.ac.uk>
 */

class OpenAnnotationSetRESTController extends BaseController {

	// Services
	def grailsApplication
	def jenaVirtuosoStoreService
	def apiKeyAuthenticationService
	def annotationIntegratedStorageService
	def openAnnotationSetsUtilsService
	def openAnnotationStorageService
	def configAccessService
	def openAnnotationVirtuosoService
	def openAnnotationUtilsService

	// Shared variables
	def apiKey
	def annotationSet // The annotationSet being operated on
	def annotationSetURI
	def startTime

	def beforeInterceptor = {
		startTime = System.currentTimeMillis()

		// Authenticate with OAuth or Annotopia API Key
		def oauthToken = apiKeyAuthenticationService.getOauthToken(request)
		if(oauthToken != null) {
			apiKey = oauthToken.system.apikey
		} else {
			apiKey = apiKeyAuthenticationService.getApiKey(request)

			if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
				invalidApiKey(request.getRemoteAddr())
				return false // Returning false stops the actual controller action from being called
			}
		}
		log.info("API key [" + apiKey + "]")

		// Figure out the URL of the annotation set
		annotationSetURI = null
		// Strip off the params to get the URL as it would appear as a named graph in the triplestore
		Matcher matcher = getCurrentUrl(request) =~ /(.*\/s\/annotationset\/[^\/]*).*/
		if(matcher.matches())
			annotationSetURI = matcher.group(1)

		// Fetch the annotation set
		annotationSet = null
		if(annotationSetURI != null) {
			log.info("Annotation set URI: " + annotationSetURI)
			annotationSet = annotationIntegratedStorageService.retrieveAnnotationSet(apiKey, annotationSetURI)
		}
		if(annotationSet == null || !annotationSet.listNames().hasNext()) {
			// Annotation Set not found
			def message = 'Annotation set ' + getCurrentUrl(request) + ' has not been found'

			render(status: 404, text: returnMessage(apiKey, "notfound", message, startTime), contentType: "text/json", encoding: "UTF-8")
			return false
		}
	}

	// TODO: Extract all this logic out into a service
	def replaceAnnotationSet = {
		def annotationSetJson = request.JSON

		def jsonURI = annotationSetJson.get("@id")
		if(jsonURI != annotationSetURI) {
			def message = "The ID of the annotation set ("+annotationSetURI+") does not match the one in the request body ("+jsonURI+")"
			log.error("[" + apiKey + "] " + message + ": " + annotationSetJson.toString())
			render(status: 500, text: returnMessage(apiKey, "invalidcontent", message, startTime), contentType: "text/json", encoding: "UTF-8")
			return
		}

		// Get the [graph URIs : URIs] of the annotations already in the set
		def existingAnnotationURIMap = annotationIntegratedStorageService.retrieveAnnotationUrisInSet(apiKey, annotationSetURI)

		// Snip out the "annotations" property
		List annotations = annotationSetJson.getAt("annotations")
		def storedAnnotationGraphURIs = []
		// Iterate over all the annotations given in the PUT and either update or create them, recording
		//  their graph URIs.
		annotations.each() {
			// Copy the @context node so the "snipped out" JSON-LD makes sense... must be a better way of doing this!
			def annotationJson = it.put("@context", annotationSetJson.get("@context"))
			def annotationDataset = DatasetFactory.createMem()
			try {
				RDFDataMgr.read(annotationDataset, new ByteArrayInputStream(annotationJson.toString().getBytes("UTF-8")), RDFLanguages.JSONLD)
			} catch (Exception ex) {
				log.error("[" + apiKey + "] " + ex.getMessage())
				def message = "Invalid content, the following annotation could not be read\n"
				log.error("[" + apiKey + "] " + message + ": " + annotationJson.toString())
				render(status: 500, text: returnMessage(apiKey, "invalidcontent", message, startTime), contentType: "text/json", encoding: "UTF-8")
				return
			}

			// Get the annotation's ID
			Set<Resource> annotationURIs = new HashSet<Resource>()
			def numAnnotations = openAnnotationUtilsService.detectAnnotationsInDefaultGraph(apiKey, annotationDataset, annotationURIs, null)

			// Hopefully there was only one annotation in the JSON-LD, but if not lets just pick the first one
			def annotationURI = null
			if(!annotationURIs.isEmpty())
				annotationURI = annotationURIs.first().toString()

			// Was the annotation already in the set?
			// TODO: check not part of another annotation set!
			def annotation
			if(annotationURI != null && existingAnnotationURIMap.remove(annotationURI) != null) {
				log.info("[" + apiKey + "] Updating annotation: " + annotationURI)
				annotation = openAnnotationStorageService.updateAnnotationDataset(apiKey, startTime, false, annotationDataset);
			} else {
				log.info("[" + apiKey + "] Adding new annotation")
				annotation = openAnnotationStorageService.saveAnnotationDataset(apiKey, startTime, false, annotationDataset);
			}
			// Record graph URI for each annotation
			// Hopefully the only named graph in the dataset is that of the annotation!
			storedAnnotationGraphURIs.push(annotation.listNames().next())
		}

		annotationSetJson.getAt("annotations").clear() // Don't need this anymore, annotations have been stored in their own graphs

		def newAnnotationSet = DatasetFactory.createMem() // Dataset for the updated annotation set
		def newAnnotationSetModel = ModelFactory.createDefaultModel() // Model for the updated annotation set
		def annotationSetGraphURI = annotationSet.listNames().next() // URI of the original set's graph

		// Read the PUT body
		try {
			RDFDataMgr.read(newAnnotationSetModel, new ByteArrayInputStream(annotationSetJson.toString().getBytes("UTF-8")), RDFLanguages.JSONLD)
		} catch (Exception ex) {
			log.error("[" + apiKey + "] " + ex.getMessage())
			def message = "Invalid content, the annotation set could not be read\n"
			log.error("[" + apiKey + "] " + message + ": " + annotationSetJson.toString())
			render(status: 500, text: returnMessage(apiKey, "invalidcontent", message, startTime), contentType: "text/json", encoding: "UTF-8")
			return
		}

		// Add annotation graph URIs to annotation set's "annotations" list
		newAnnotationSetModel.removeAll(ResourceFactory.createResource(annotationSetURI),
				ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATIONS),
				null)
		storedAnnotationGraphURIs.each() {
			newAnnotationSetModel.add(ResourceFactory.createResource(annotationSetURI),
					ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATIONS),
					ResourceFactory.createResource(it))
		}

		// Set Last saved on
		newAnnotationSetModel.removeAll(ResourceFactory.createResource(annotationSetURI),
				ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON),
				null)
		newAnnotationSetModel.add(ResourceFactory.createResource(annotationSetURI),
				ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON),
				ResourceFactory.createPlainLiteral(dateFormat.format(new Date())))

		// Add the updated annotation set as a named graph to the dataset
		newAnnotationSet.addNamedModel(annotationSetGraphURI, newAnnotationSetModel)

		// Store the updated annotation set dataset
		jenaVirtuosoStoreService.updateDataset(apiKey, newAnnotationSet)

		// Remove now-orphaned annotations
		existingAnnotationURIMap.values().each() {
			log.info("[" + apiKey + "] Deleting orphaned annotation graph: " + it);
			jenaVirtuosoStoreService.dropGraph(apiKey, it);
			jenaVirtuosoStoreService.removeAllTriples(apiKey, configAccessService.getAsString("annotopia.storage.uri.graph.provenance"), it);
			jenaVirtuosoStoreService.removeAllTriplesWithObject(apiKey, it);
		}

		// Render the set
		openAnnotationSetsUtilsService.renderAnnotationSet(apiKey, newAnnotationSet, response, 200)
	}

	// Create an annotation in a set
	def createAnnotation = {
		def item = request.JSON

		// Add annotation to the triplestore
		def annotation = DatasetFactory.createMem()
		try {
			RDFDataMgr.read(annotation, new ByteArrayInputStream(item.toString().getBytes("UTF-8")), RDFLanguages.JSONLD)
		} catch (Exception ex) {
			log.error("[" + apiKey + "] " + ex.getMessage())
			def message = "Invalid content, annotation cannot be read"
			log.error("[" + apiKey + "] " + message + ": " + item.toString())
			render(status: 500, text: returnMessage(apiKey, "invalidcontent", message, startTime), contentType: "text/json", encoding: "UTF-8")
			return
		}
		// Store the annotation
		def storedAnnotation = openAnnotationStorageService.saveAnnotationDataset(apiKey, startTime, false, annotation)

		// Add the annotation to the set
		def annotationGraphURI = storedAnnotation.listNames().next()
		def annotationSetGraphURI = annotationSet.listNames().next()

		def annotationSetModel = annotationSet.getNamedModel(annotationSetGraphURI)
		annotationSetModel.add(ResourceFactory.createResource(annotationSetURI),
				ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATIONS),
				ResourceFactory.createResource(annotationGraphURI))
		// Set Last saved on
		annotationSetModel.removeAll(ResourceFactory.createResource(annotationSetURI), ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON), null);
		annotationSetModel.add(ResourceFactory.createResource(annotationSetURI), ResourceFactory.createProperty(PAV.PAV_LAST_UPDATED_ON),
			ResourceFactory.createPlainLiteral(dateFormat.format(new Date())));

		// TODO: Needs to increment PAV_VERSION

		// Store the updated annotation set
		jenaVirtuosoStoreService.updateDataset(apiKey, annotationSet)

		// Render the set
		// TODO: Maybe just render the created annotation?
		openAnnotationSetsUtilsService.renderAnnotationSet(apiKey, annotationSet, response, 201)
	}
}
