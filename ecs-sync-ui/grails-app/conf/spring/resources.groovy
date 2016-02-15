import grails.plugins.rest.client.RestBuilder
import sync.ui.CustomPropertyEditorRegistrar

// Place your Spring DSL code here
beans = {
    rest(RestBuilder)//, [proxy: ['192.168.3.132': 8888]])
    jobServer(String, "http://localhost:9200")//"http://192.168.3.1:9200")
    customPropertyEditorRegistrar(CustomPropertyEditorRegistrar)
    iso8601Format(String, "yyyy-MM-dd'T'HH:mm:ssX")
}
