# Event Store Client
Contains the event sourcing clients implementations.

## Dependency Information
This project uses the reference API for event sourcing, as below:

```xml
<dependency>
    <groupId>uk.co.blackcell.eventsourcing</groupId>
    <artifactId>eventstore-api</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

Include the following in your pom.xml to take on this dependency:

```xml
<dependency>
    <groupId>gov.dvla.osl</groupId>
    <artifactId>eventstore-client</artifactId>
    <version>1.0.0-SNAPSHOT</version>
</dependency>
```

Projection Lombok deserialisation support added by following dependencies, Note that this requirement vanishes once Jackson 2.7 is in use - it's a workaround because Jackson 2.6 doesn't recognise the ConstructorProperties stuff

```xml
<dependency>
    <groupId>org.projectlombok</groupId>
    <artifactId>lombok</artifactId>
    <version>1.16.8</version>
    <scope>provided</scope>
</dependency>

<dependency>
    <groupId>com.xebia</groupId>
    <artifactId>jackson-lombok</artifactId>
    <version>1.1</version>
</dependency>
```