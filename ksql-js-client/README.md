# ksqlDB-js-client

| :warning:	Proof of concept |
| -------------------------- |
| This directory provides a sample ksqlDB JavaScript client and application, based on the [KLIP-15 proposal](https://github.com/confluentinc/ksql/pull/4069). |


### Running the example app

You'll need a recent version of Node.js. Use npm (or yarn) to grab dependencies and compile the client:
```sh
# within the ksqldb-js-client/ dir
$ npm run build
```

The sample application in `ksqldb-js-client/packages/shopping-cart-example` emulates the `ksqldb-real-app` example from KLIP-15. It reuses the server endpoint, so fire that up first:

> To run it, run the Starter class in your IDE: [...]/ksql-real-app/src/main/java/io/confluent/server/Starter.java 

(From `# The Application` in [ksql-dev / Client, Protocol, and new API prototype](https://groups.google.com/d/msg/ksql-dev/5mLKvtZFs4Y/HUAtYNw4AgAJ).)

Then, start the JavaScript-client-based example:
```sh
# within the ksqldb-js-client/ dir
$ npm run start-example
```

You should see output like:
```
Server running at http://localhost:1234
```

Finally, navigate your browser to the respective url.
