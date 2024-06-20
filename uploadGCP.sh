mvn clean package
gcloud compute scp target/serverlessApp-1.0-jar-with-dependencies.jar sps-storm-nimbus:~/scripts/serverlessApp.jar