# Apple Music Feed Java Example

Script for how to get urls from the latest dump for Apple Music Feed, store them as files, and then load them in parquet
and analyze them.

## Build
Build with
`./gradlew build`

## Running:

### Run With Gradle

Designed for Java 17 and gradle.

Run with 

`./gradlew run`

For help, pass in the `-h` arg

`./gradlew run --args='-h'`
for a description of the different arguments
Pass in further arguments there:

`./gradlew run --args='--key-id=$KEY_ID --team-id=$TEAM_ID --secret-key-file-path=fake/file/path/private_key_secret.p8 --out-dir=/tmp/amf'`

### Java Jar

Create a jar with
```bash
./gradlew jar
java -jar  app/build/libs/app.jar
```

