package org.apache.nifi.jasn1;

import com.beanit.jasn1.ber.ReverseByteArrayOutputStream;
import com.beanit.jasn1.ber.types.BerBoolean;
import com.beanit.jasn1.ber.types.BerInteger;
import com.beanit.jasn1.ber.types.BerOctetString;
import org.apache.nifi.jasn1.example.BasicTypes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;

public class ExampleDataGenerator {

    private static final Logger LOG = LoggerFactory.getLogger(ExampleDataGenerator.class);

    public static void main(String[] args) throws Exception {

        final File asnFile = new File(ExampleDataGenerator.class.getResource("/example.asn").getFile());
        final File dir = new File(asnFile.getParentFile().getParentFile().getParentFile(), "src/test/resources/examples");

        try (final ReverseByteArrayOutputStream rev = new ReverseByteArrayOutputStream(1024);
            final OutputStream out = new FileOutputStream(new File(dir, "basic-types.dat"))) {
            final BasicTypes basicTypes = new BasicTypes();
            basicTypes.setB(new BerBoolean(true));
            basicTypes.setI(new BerInteger(789));
            basicTypes.setOctStr(new BerOctetString(new byte[]{1, 2, 3, 4, 5}));
            final int encoded = basicTypes.encode(rev);
            out.write(rev.getArray(), 0, encoded);
        }

    }
}
