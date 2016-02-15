package sync.ui

import org.springframework.beans.PropertyEditorRegistrar
import org.springframework.beans.PropertyEditorRegistry
import org.springframework.beans.propertyeditors.CustomDateEditor

import java.text.SimpleDateFormat

class CustomPropertyEditorRegistrar implements PropertyEditorRegistrar {
    @Override
    void registerCustomEditors(PropertyEditorRegistry registry) {
        registry.registerCustomEditor(Date.class, new CustomDateEditor(new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssX"), true));
    }
}