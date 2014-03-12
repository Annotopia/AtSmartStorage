class UrlMappings {

	static mappings = {
		"/s/annotationset/$id?"{
			controller = "openAnnotationSet"
			action = [GET:"show", POST:"save", PUT:"update"]
		}
		"/s/annotation/$id?"{
			controller = "openAnnotation"
			action = [GET:"show", POST:"save", PUT:"update"]
		}
		
		"/oa/validate"{
			controller = "openAnnotation"
			action = "validate"
		}
		"/oa/search"{
			controller = "openAnnotation"
			action = "search"
		}
		
		"/api/stats/$action?" {
			controller = "openAnnotationReporting"
		}

		"/$controller/$action?/$id?"{
			constraints {
				// apply constraints here
			}
		}

		"/"(view:"/index")
		"500"(view:'/error')
	}
}
