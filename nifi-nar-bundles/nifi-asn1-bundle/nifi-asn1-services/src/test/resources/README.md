## Updating example.asn file

The unit tests and generated sources rely on the definition of `example.asn` file. When you update the file to fix/update/add unit tests, perform following operations to update derived resources.

1. Generate source files for unit test:

    ```
    $ jasn1/bin/jasn1-compiler -f example.asn -o ./../java
    ```

2. To generate 'test/resources/examples/*.dat' files, execute `ExampleDataGenerator`.