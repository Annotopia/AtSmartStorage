class UrlMappings {

	static mappings = {
		"/s/annotationset/$id?"{
			controller = "annotationSet"
			action = [GET:"show", POST:"save", PUT:"update"]
		}
		"/s/annotation/$id?"{
			controller = "openAnnotation"
			action = [GET:"show", POST:"save", PUT:"update"]
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
