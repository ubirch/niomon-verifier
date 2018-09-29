FROM ubirch/java
MAINTAINER Matthias L. Jugel <matthias.jugel@ubirch.com>
ARG JAR_LIBS
ARG JAR_FILE

EXPOSE 8080
EXPOSE 9010

ENV _JAVA_OPTIONS "-Xms128m -Xmx256m -Djava.awt.headless=true"

ENTRYPOINT [ \
  "/usr/bin/java", \
  "-Djava.security.egd=file:/dev/./urandom", \
  "-Djava.rmi.server.hostname=localhost", \
  "-Dcom.sun.management.jmxremote", \
  "-Dcom.sun.management.jmxremote.port=9010", \
  "-Dcom.sun.management.jmxremote.rmi.port=9010", \
  "-Dcom.sun.management.jmxremote.local.only=false", \
  "-Dcom.sun.management.jmxremote.authenticate=false", \
  "-Dcom.sun.management.jmxremote.ssl=false", \
  "-jar", "/usr/share/service/main.jar" \
]

# Add Maven dependencies (not shaded into the artifact; Docker-cached)
COPY ${JAR_LIBS} /usr/share/service/lib
# Add the service itself
COPY ${JAR_FILE} /usr/share/service/main.jar