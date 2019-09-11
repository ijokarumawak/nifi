package org.apache.nifi.jasn1;

import org.apache.nifi.annotation.lifecycle.OnEnabled;
import org.apache.nifi.components.AbstractConfigurableComponent;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.components.PropertyValue;
import org.apache.nifi.controller.ConfigurationContext;
import org.apache.nifi.controller.ControllerServiceInitializationContext;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.logging.ComponentLog;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.reporting.InitializationException;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.util.file.classloader.ClassLoaderUtils;

import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class JASN1Reader extends AbstractConfigurableComponent implements RecordReaderFactory {

    private static PropertyDescriptor ROOT_CLASS_NAME = new PropertyDescriptor.Builder()
        .name("root-model-class-name")
        .displayName("Root Model Class Name")
        .description("A canonical class name that is generated by jASN1 compiler to encode the ASN1 input data.")
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .required(true)
        .build();

    private static final PropertyDescriptor MODEL_CLASS_PATHS = new PropertyDescriptor.Builder()
        .name("jasn1-model-class-paths")
        .displayName("jASN1 Model Class Paths")
        .description("Comma-separated list of paths to jar files and/or directories which contain model classes those are generated by jASN1 compiler using a ASN1 definition file (that are not included on NiFi's classpath).")
        .required(false)
        .expressionLanguageSupported(ExpressionLanguageScope.VARIABLE_REGISTRY)
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .build();

    private final List<PropertyDescriptor> propertyDescriptors = Arrays.asList(
        ROOT_CLASS_NAME,
        MODEL_CLASS_PATHS
    );

    private String identifier;
    private ComponentLog logger;

    private RecordSchemaProvider schemaProvider = new RecordSchemaProvider();

    private volatile ClassLoader customClassLoader;
    private volatile PropertyValue rootClassNameProperty;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        return propertyDescriptors;
    }

    @Override
    public void initialize(ControllerServiceInitializationContext context) throws InitializationException {
        identifier = context.getIdentifier();
        logger = context.getLogger();
    }

    @OnEnabled
    public void onEnabled(final ConfigurationContext context) throws InitializationException {

        try {
            if (context.getProperty(MODEL_CLASS_PATHS).isSet()) {
                customClassLoader = ClassLoaderUtils.getCustomClassLoader(context.getProperty(MODEL_CLASS_PATHS)
                    .evaluateAttributeExpressions().getValue(), this.getClass().getClassLoader(), getJarFilenameFilter());
            } else {
                customClassLoader = this.getClass().getClassLoader();
            }
        } catch (final Exception ex) {
            logger.error("Unable to setup JASN1Reader", ex);
        }

        rootClassNameProperty = context.getProperty(ROOT_CLASS_NAME);
    }

    @Override
    public String getIdentifier() {
        return identifier;
    }

    private FilenameFilter getJarFilenameFilter(){
        return (dir, name) -> (name != null && name.endsWith(".jar"));
    }

    @Override
    public RecordReader createRecordReader(Map<String, String> variables, InputStream in, long inputLength, ComponentLog logger) throws MalformedRecordException, IOException, SchemaNotFoundException {

        final String rootClassName = rootClassNameProperty.evaluateAttributeExpressions(variables).getValue();
        return new JASN1RecordReader(rootClassName, schemaProvider, customClassLoader, in, logger);
    }


}
