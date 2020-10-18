# WiseKG-client
A multi-threaded WiseKG client written in Java 8.

## Build
Execute the following command to create a JAR file:
```
$ mvn install
```

## Usage
Use the following command
```
java -jar [filename].jar -t false -f [Starting Fragment] -q [Query File]
```

## Run tests
To run tests similar to the ones in the paper, you must run the jar file once per client you are running. Then, use the following command per client.
```
java -jar [filename].jar -t true [Starting Fragment] [query directory] [method] [output dir] [number of clients] [client no.] [dataset] [load]
```

In the [output directory], there must be one subdirectory per query load entitled [load].

The full test setup used in the paper will be available on [https://github.com/WiseKG/WiseKG-Experiments](https://github.com/WiseKG/WiseKG-Experiments).
