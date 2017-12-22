import grails.plugins.rest.client.RestBuilder
import org.springframework.web.client.RestTemplate
import sync.ui.ArrayToStringByLineConverter
import sync.ui.StringToArrayByLineConverter
import sync.ui.SyncHttpMessageConverter

// proxy config
//def reqFactory = new SimpleClientHttpRequestFactory()
//reqFactory.proxy = new Proxy(Proxy.Type.HTTP, new InetSocketAddress("localhost", 8888))

def restTemplate = new RestTemplate()//reqFactory)

// custom XML converter to support marshalling dynamic plugins
restTemplate.getMessageConverters().removeAll {
    it.getClass().name == "org.springframework.http.converter.xml.Jaxb2RootElementHttpMessageConverter"
}
restTemplate.getMessageConverters().add(new SyncHttpMessageConverter())

// Place your Spring DSL code here
beans = {
    arrayToStringLineConverter(ArrayToStringByLineConverter)
    stringToArrayLineConverter(StringToArrayByLineConverter)
    rest(RestBuilder, restTemplate)
    jobServer(String, "http://localhost:9200")
    syncHttpMessageConverter(SyncHttpMessageConverter)
}
