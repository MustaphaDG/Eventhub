# Eventhub

Simple Java CLI for sending and receiving messages with Azure Event Hubs.

## Prerequisites
- Java 11+
- Maven 3.6+

## Configuration
Set the following environment variables:
- `EVENTHUB_CONNECTION_STRING` (required)
- `EVENTHUB_NAME` (required if the connection string does not include `EntityPath`)
- `EVENTHUB_CONSUMER_GROUP` (optional, defaults to `$Default`)
- `EVENTHUB_PARTITION_ID` (optional, defaults to the first partition when receiving)

## Build
```bash
mvn clean package
```

## Send messages
```bash
java -jar target/eventhub-app-<version>.jar send "Hello Event Hubs" "Another message"
```
Replace `<version>` with the version from `pom.xml` (for example, `1.0-SNAPSHOT`).

## Receive messages
```bash
java -jar target/eventhub-app-<version>.jar receive
```
