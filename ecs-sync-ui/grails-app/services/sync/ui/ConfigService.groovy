package sync.ui

import grails.transaction.Transactional

@Transactional(readOnly = true)
abstract class ConfigService {
    abstract List<String> listConfigObjects(String path)

    abstract boolean configObjectExists(String path)

    abstract <T> T readConfigObject(String path, Class<T> resultType)

    abstract void writeConfigObject(String path, content, String contentType)

    abstract void deleteConfigObject(String path)

    abstract URI configObjectQuickLink(String path)

    abstract void writeConfig(UiConfig uiConfig)

    abstract void readConfig(UiConfig uiConfig)

    UiConfig readConfig() {
        def uiConfig = getConfig()
        readConfig(uiConfig)
        return uiConfig
    }

    static UiConfig getConfig() {
        def uiConfig = UiConfig.first([readOnly: true])
        if (uiConfig == null) throw new ConfigException("Missing configuration")
        return uiConfig
    }
}
