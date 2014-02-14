class UrlMappings {

	static mappings = {
		"/storage/annotationset/$id?"{
			controller = "annotationSet"
			action = [GET:"show", POST:"save", PUT:"update"]
		}
		"/storage/annotation/$id?"{
			controller = "annotation"
			action = [GET:"show", POST:"save"]
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
