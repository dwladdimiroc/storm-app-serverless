mvn clean package
storm kill -w 0 serverlessApp
storm jar target/serverlessApp-1.0-jar-with-dependencies.jar com.github.dwladdimiroc.serverlessApp.topology.Topology constant 10
