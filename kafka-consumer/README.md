# Kafka Consumer Application

## JMX Monitoring with JConsole

### Prerequisites
1. Java Development Kit (JDK) installed on your machine
2. The kafka-consumer container running
3. JConsole available in your Java installation's bin directory

### Connecting with JConsole

1. **Start JConsole**
   - Open a terminal/command prompt
   - Run the following command:
   ```bash
   jconsole
   ```
   - If jconsole is not found, use the full path:
   ```bash
   "C:\Program Files\Java\jdk-{version}\bin\jconsole.exe"
   ```
   (Replace {version} with your JDK version)

2. **Connect to Kafka Consumer**
   - In JConsole, select "Remote Process"
   - Enter the connection string:
   ```
   service:jmx:rmi:///jndi/rmi://localhost:9010/jmxrmi
   ```
   - Leave username and password empty (authentication is disabled)
   - Click "Connect"

3. **View Metrics**
   - Once connected, you can view:
     - Memory usage
     - CPU usage
     - Thread states
     - Classes
     - VM Summary
     - MBeans

4. **Important Kafka Metrics**
   - Navigate to MBeans tab
   - Look for:
     - `kafka.consumer` -> consumer-fetch-manager-metrics
     - `kafka.consumer` -> consumer-coordinator-metrics
   - Key metrics:
     - records-lag-max: Maximum lag in terms of number of records for any partition
     - records-lag-avg: Average lag in terms of number of records for any partition
     - records-consumed-rate: Average number of records consumed per second

### Troubleshooting

1. **Cannot Connect to JMX Port**
   - Verify the container is running:
   ```bash
   docker ps | grep kafka-consumer
   ```
   - Check if ports are exposed:
   ```bash
   docker port kafka-consumer
   ```
   - Expected output should show:
     ```
     8081/tcp -> 0.0.0.0:8081
     9010/tcp -> 0.0.0.0:9010
     9011/tcp -> 0.0.0.0:9011
     ```

2. **JConsole Not Found**
   - Ensure JAVA_HOME is set correctly
   - Add Java's bin directory to your system's PATH
   - Use the full path to jconsole.exe as shown above

### Container Configuration

The JMX configuration in the container is set up with:
```yaml
environment:
  JAVA_TOOL_OPTIONS: >-
    -Dcom.sun.management.jmxremote
    -Dcom.sun.management.jmxremote.port=9010
    -Dcom.sun.management.jmxremote.rmi.port=9011
    -Dcom.sun.management.jmxremote.authenticate=false
    -Dcom.sun.management.jmxremote.ssl=false
    -Djava.rmi.server.hostname=localhost
```

### Security Note
Current configuration has authentication and SSL disabled for simplicity. For production environments, it's recommended to:
- Enable authentication
- Configure SSL/TLS
- Restrict JMX port access through network policies 