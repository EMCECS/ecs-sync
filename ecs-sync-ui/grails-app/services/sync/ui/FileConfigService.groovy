package sync.ui

import com.google.common.base.Charsets
import grails.transaction.Transactional
import groovy.io.FileType
import org.springframework.beans.BeanUtils

import javax.xml.bind.JAXBContext

@Transactional(readOnly = true)
class FileConfigService extends ConfigService {
    def jaxbContext = JAXBContext.newInstance(SyncHttpMessageConverter.pluginClasses)

    def grailsLinkGenerator

    @Override
    List<String> listConfigObjects(String path) {
        def files = []
        def dir = new File(filePath(path))
        if (dir.exists()) dir.eachFile(FileType.FILES) { files << it.path }
        files
    }

    @Override
    boolean configObjectExists(String path) {
        def file = new File(filePath(path))
        return file.exists() && file.isFile()
    }

    @Override
    <T> T readConfigObject(String path, Class<T> resultType) {
        def file = new File(filePath(path))
        if (resultType == byte[].class) {
            return file.getBytes() as T
        } else if (resultType == InputStream.class) {
            return file.newInputStream() as T
        } else if (resultType == String.class) {
            return new String(file.getBytes(), Charsets.UTF_8) as T
        } else { // try unmarshalling
            return jaxbContext.createUnmarshaller().unmarshal(file) as T
        }
    }

    @Override
    void writeConfigObject(String path, Object content, String contentType) {
        def file = new File(filePath(path))
        if (file.parentFile) file.parentFile.mkdirs()
        if (content instanceof byte[]) {
            file.bytes = content as byte[]
        } else if (content instanceof InputStream) {
            file.withOutputStream {
                it << (content as InputStream)
                (content as InputStream).close()
            }
        } else if (content instanceof String) {
            file.write(content as String)
        } else { // try marshalling
            jaxbContext.createMarshaller().marshal(content, file)
        }
    }

    @Override
    void deleteConfigObject(String path) {
        new File(filePath(path)).delete()
    }

    @Override
    URI configObjectQuickLink(String path) {
        grailsLinkGenerator.resource(dir: 'storage', file: path).toURI()
    }

    @Override
    void writeConfig(UiConfig uiConfig) {
        File file = new File(uiConfig.filePath, "ui-config.xml")
        if (file.parentFile) file.parentFile.mkdirs()
        jaxbContext.createMarshaller().marshal(uiConfig, file)
    }

    @Override
    void readConfig(UiConfig uiConfig) {
        File file = new File(uiConfig.filePath, "ui-config.xml")
        BeanUtils.copyProperties(jaxbContext.createUnmarshaller().unmarshal(file), uiConfig, 'id')
    }

    String filePath(String path) {
        UiConfig uiConfig = getConfig()
        new File(uiConfig.filePath, path).path
    }
}
