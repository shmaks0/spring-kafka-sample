# spring-kafka-sample
Simple spring boot app with consumer for Kafka

Steps to run locally:
1) set HOST_NAME variable in .env file
2) run docker-compose up -d (it setup kafka and postgres)
3) after pg is ready run ./run.sh

Alternatively you can specify params for kafka and postgres in application.properties

Simple integration tests can be found in KafkaSampleApplicationTest class, 
they're excluded from default test task of gradle, please use integrationTest task to run them 
