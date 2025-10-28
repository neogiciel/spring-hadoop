FROM openjdk:17
VOLUME /tmp

# Copy the compiled JAR file to the image
#COPY target/spring-hadoop-0.0.1-SNAPSHOT.jar /app/spring-hadoop.jar
#RUN useradd -u 185 sparkuser
EXPOSE 8080
ARG JAR_FILE=target/spring-hadoop-0.0.1-SNAPSHOT.jar
ADD ${JAR_FILE} spring-hadoop.jar
#ENTRYPOINT ["java","-jar","/spring-hadoop.jar"]
ENTRYPOINT ["java", \
            "--add-opens", "java.base/java.lang=ALL-UNNAMED", \
            "--add-opens", "java.base/java.util=ALL-UNNAMED", \
            "--add-opens", "java.base/java.lang.reflect=ALL-UNNAMED", \
            "--add-opens", "java.base/java.text=ALL-UNNAMED", \
            "--add-opens", "java.desktop/java.beans=ALL-UNNAMED", \
            "--add-opens", "java.desktop/java.awt.font=ALL-UNNAMED", \
            "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED", \
            "--add-opens", "java.base/java.nio=ALL-UNNAMED", \
            "--add-opens", "java.base/java.util=ALL-UNNAMED", \
            "--add-opens", "java.base/sun.nio.ch=ALL-UNNAMED", \
            "--add-opens", "java.base/java.io=ALL-UNNAMED", \
            "-jar","/spring-hadoop.jar"]


