package org.apache.nifi.jasn1;

import com.beanit.jasn1.ber.ReverseByteArrayOutputStream;
import com.beanit.jasn1.ber.types.BerBoolean;
import com.beanit.jasn1.ber.types.BerInteger;
import com.beanit.jasn1.ber.types.BerOctetString;
import org.apache.nifi.jasn1.example.BasicTypes;
import org.apache.nifi.jasn1.example.Composite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class ExampleDataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(ExampleDataGenerator.class);

    public static void main(String[] args) throws Exception {

        final File asnFile = new File(ExampleDataGenerator.class.getResource("/example.asn").getFile());
        final File dir = new File(asnFile.getParentFile().getParentFile().getParentFile(), "src/test/resources/examples");

        generateBasicTypes(dir);

        generateComposite(dir);
    }

    private static void generateBasicTypes(File dir) throws IOException {
        final File file = new File(dir, "basic-types.dat");
        try (final ReverseByteArrayOutputStream rev = new ReverseByteArrayOutputStream(1024);
             final OutputStream out = new FileOutputStream(file)) {
            final BasicTypes basicTypes = new BasicTypes();
            basicTypes.setB(new BerBoolean(true));
            basicTypes.setI(new BerInteger(789));
            basicTypes.setOctStr(new BerOctetString(new byte[]{1, 2, 3, 4, 5}));
            final int encoded = basicTypes.encode(rev);
            out.write(rev.getArray(), 0, encoded);
            LOG.info("Generated {} bytes to {}", encoded, file);
        }
    }

    private static void generateComposite(File dir) throws IOException {
        final File file = new File(dir, "composite.dat");
        try (final ReverseByteArrayOutputStream rev = new ReverseByteArrayOutputStream(1024);
             final OutputStream out = new FileOutputStream(file)) {
            final Composite composite = new Composite();
            BasicTypes child = new BasicTypes();
            child.setB(new BerBoolean(true));
            child.setI(new BerInteger(789));
            child.setOctStr(new BerOctetString(new byte[]{1, 2, 3, 4, 5}));

            composite.setChild(child);

            final Composite.Children children = new Composite.Children();
            composite.setChildren(children);
            for (int i = 0; i < 3; i++) {
                child = new BasicTypes();
                child.setB(new BerBoolean(i % 2 == 0));
                child.setI(new BerInteger(i));
                child.setOctStr(new BerOctetString(new byte[]{(byte) i, (byte) i, (byte) i}));
                children.getBasicTypes().add(child);
            }

            final Composite.Numbers numbers = new Composite.Numbers();
            composite.setNumbers(numbers);
            numbers.getBerInteger().addAll(IntStream.range(0, 4)
                .mapToObj(BerInteger::new).collect(Collectors.toList()));

            final int encoded = composite.encode(rev);
            out.write(rev.getArray(), 0, encoded);
            LOG.info("Generated {} bytes to {}", encoded, file);
        }
    }
}
