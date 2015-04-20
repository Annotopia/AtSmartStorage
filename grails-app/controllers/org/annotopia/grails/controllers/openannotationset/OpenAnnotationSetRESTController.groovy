package org.annotopia.grails.controllers.openannotationset

import java.util.regex.Matcher
import org.annotopia.grails.vocabularies.AnnotopiaVocabulary
import org.annotopia.groovy.service.store.BaseController
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.RDFLanguages

import com.hp.hpl.jena.query.DatasetFactory
import com.hp.hpl.jena.rdf.model.Model
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

	// Shared variables
	def apiKey
	def annotationSet // The annotationSet being operated on
	def annotationSetURI
	def startTime

	def beforeInterceptor = {
		startTime = System.currentTimeMillis()
		
		// Authenticate
		Matcher matcher = request.getHeader("Authorization") =~ /.*annotopia-api-key\s+([-0-9a-fA-F]*).*/
        if(matcher.matches())
		  apiKey = matcher.group(1)
	  	else
		  apiKey = params.apiKey

		if(!apiKeyAuthenticationService.isApiKeyValid(request.getRemoteAddr(), apiKey)) {
			invalidApiKey(request.getRemoteAddr())
			return false // Returning false stops the actual controller action from being called
		}

		log.info("API key [" + apiKey + "]")

		// Figure out the URL of the annotation set
		annotationSetURI = null
		// Strip off the params to get the URL as it would appear as a named graph in the triplestore
		if(getCurrentUrl(request).indexOf("/annotations/") > 0)
			annotationSetURI = getCurrentUrl(request).substring(0, getCurrentUrl(request).indexOf("/annotations/"))
		else
			annotationSetURI = getCurrentUrl(request)

		log.info(annotationSetURI)

		// Fetch the annotation set
		annotationSet = annotationIntegratedStorageService.retrieveAnnotationSet(apiKey, annotationSetURI)
		if(annotationSet == null || !annotationSet.listNames().hasNext()) {
			// Annotation Set not found
			def message = 'Annotation set ' + getCurrentUrl(request) + ' has not been found'

			render(status: 404, text: returnMessage(apiKey, "notfound", message, startTime), contentType: "text/json", encoding: "UTF-8")
			return false
		}
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
		annotationSet.getNamedModel(annotationSetGraphURI).add(ResourceFactory.createResource(annotationSetURI),
				ResourceFactory.createProperty(AnnotopiaVocabulary.ANNOTATIONS),
				ResourceFactory.createResource(annotationGraphURI))
		// Store the updated annotation set
		jenaVirtuosoStoreService.storeDataset(apiKey, annotationSet)

		// Render the set
		openAnnotationSetsUtilsService.renderSavedNamedGraphsDataset(apiKey, startTime, 'none', 'saved', response, annotationSet)
	}
}
