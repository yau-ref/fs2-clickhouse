# tests

Unit and testcontainers-based integration tests against a real ClickHouse instance.

Some integration tests use ScalaCheck for property-based testing — generating randomized rows (e.g. `TestData.animalGen`) to insert into and query back from ClickHouse, verifying round-trip encoding/decoding across a wide range of inputs rather than a handful of fixed examples.
