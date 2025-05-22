"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.handler = void 0;
const kafkajs_1 = require("kafkajs");
const client_cloudwatch_1 = require("@aws-sdk/client-cloudwatch");
const client_secrets_manager_1 = require("@aws-sdk/client-secrets-manager");
// Initialize AWS clients
const cloudWatch = new client_cloudwatch_1.CloudWatchClient({});
const secretsManager = new client_secrets_manager_1.SecretsManagerClient({});
async function getConsumerLag(kafka, consumerGroup, topic) {
    const admin = kafka.admin();
    try {
        await admin.connect();
        // Get topic partitions
        const metadata = await admin.fetchTopicMetadata({ topics: [topic] });
        const topicMetadata = metadata.topics.find(t => t.name === topic);
        if (!topicMetadata || topicMetadata.partitions.length === 0) {
            console.warn(`No partitions found for topic ${topic}`);
            return null;
        }
        const partitions = topicMetadata.partitions.map(p => p.partitionId);
        // Get end offsets
        const endOffsets = await admin.fetchTopicOffsets(topic);
        if (!endOffsets || endOffsets.length === 0) {
            console.warn(`No end offsets found for topic ${topic}`);
            return null;
        }
        // Get consumer group offsets
        const groupOffsets = await admin.fetchOffsets({ groupId: consumerGroup, topics: [topic] });
        if (!groupOffsets || groupOffsets.length === 0) {
            console.warn(`No offsets found for consumer group ${consumerGroup} on topic ${topic}`);
            return null;
        }
        // Calculate total lag
        let totalLag = 0;
        const consumerPartitions = groupOffsets[0].partitions;
        for (const partition of partitions) {
            const endOffset = endOffsets.find(o => o.partition === partition)?.offset || '0';
            const consumerOffset = consumerPartitions.find(p => p.partition === partition)?.offset || '0';
            const lag = parseInt(endOffset) - parseInt(consumerOffset);
            if (lag > 0) {
                totalLag += lag;
            }
        }
        console.info(`Calculated lag for ${consumerGroup} on ${topic}: ${totalLag} messages across ${partitions.length} partitions`);
        return totalLag;
    }
    catch (error) {
        console.error(`Error collecting lag for ${consumerGroup} on ${topic}:`, error);
        return null;
    }
    finally {
        await admin.disconnect();
    }
}
async function publishMetric(consumerGroup, topic, lagValue, namespace) {
    try {
        const command = new client_cloudwatch_1.PutMetricDataCommand({
            Namespace: namespace,
            MetricData: [{
                    MetricName: 'custom.kafka.consumer.lag',
                    Value: lagValue,
                    Unit: 'Count',
                    Dimensions: [
                        { Name: 'consumer_group', Value: consumerGroup },
                        { Name: 'topic', Value: topic },
                        { Name: 'metric_type', Value: 'consumer_lag' }
                    ],
                    StorageResolution: 60 // 1-minute resolution
                }]
        });
        await cloudWatch.send(command);
        console.info(`Published lag metric: group=${consumerGroup}, topic=${topic}, lag=${lagValue}`);
    }
    catch (error) {
        console.error('Error publishing metric to CloudWatch:', error);
        throw error;
    }
}
async function getSSLConfig() {
    if (process.env.KAFKA_SSL_ENABLED !== 'true') {
        return undefined;
    }
    try {
        let certs;
        if (process.env.KAFKA_CERTIFICATES_SECRET_ARN) {
            const command = new client_secrets_manager_1.GetSecretValueCommand({
                SecretId: process.env.KAFKA_CERTIFICATES_SECRET_ARN
            });
            const response = await secretsManager.send(command);
            certs = JSON.parse(response.SecretString || '{}');
        }
        else {
            // Use default certificate paths
            certs = {
                ca: '/opt/kafka/cert/ca.crt',
                cert: '/opt/kafka/cert/client.crt',
                key: '/opt/kafka/cert/client.key'
            };
        }
        return {
            ca: [certs.ca],
            key: certs.key,
            cert: certs.cert,
            rejectUnauthorized: true
        };
    }
    catch (error) {
        console.error('Error getting SSL configuration:', error);
        throw error;
    }
}
const handler = async (event, context) => {
    try {
        // Get configuration from environment variables
        const bootstrapServers = process.env.KAFKA_BOOTSTRAP_SERVERS;
        const namespace = process.env.CLOUDWATCH_NAMESPACE;
        if (!bootstrapServers || !namespace) {
            throw new Error('Required environment variables are not set');
        }
        // Get SSL configuration if enabled
        const ssl = await getSSLConfig();
        // Configure Kafka client
        const kafka = new kafkajs_1.Kafka({
            brokers: bootstrapServers.split(','),
            clientId: 'lag-collector',
            ssl,
            logLevel: kafkajs_1.logLevel.INFO
        });
        // Get consumer groups and topics from environment
        const consumersConfig = JSON.parse(process.env.CONSUMER_CONFIG || '[]');
        const results = [];
        for (const config of consumersConfig) {
            try {
                // Get lag for this consumer group and topic
                const lag = await getConsumerLag(kafka, config.consumer_group, config.topic);
                if (lag !== null) {
                    // Publish to CloudWatch
                    await publishMetric(config.consumer_group, config.topic, lag, namespace);
                    results.push({
                        consumer_group: config.consumer_group,
                        topic: config.topic,
                        lag,
                        status: 'success'
                    });
                }
                else {
                    results.push({
                        consumer_group: config.consumer_group,
                        topic: config.topic,
                        status: 'error',
                        message: 'Failed to calculate lag'
                    });
                }
            }
            catch (error) {
                console.error(`Failed to process ${config.consumer_group} on ${config.topic}:`, error);
                results.push({
                    consumer_group: config.consumer_group,
                    topic: config.topic,
                    status: 'error',
                    message: error instanceof Error ? error.message : 'Unknown error'
                });
            }
        }
        return {
            statusCode: 200,
            body: JSON.stringify({
                message: 'Lag metrics collection completed',
                results
            })
        };
    }
    catch (error) {
        console.error('Lambda execution failed:', error);
        return {
            statusCode: 500,
            body: JSON.stringify({
                error: error instanceof Error ? error.message : 'Unknown error'
            })
        };
    }
};
exports.handler = handler;
//# sourceMappingURL=data:application/json;base64,eyJ2ZXJzaW9uIjozLCJmaWxlIjoiaW5kZXguanMiLCJzb3VyY2VSb290IjoiIiwic291cmNlcyI6WyIuLi9zcmMvaW5kZXgudHMiXSwibmFtZXMiOltdLCJtYXBwaW5ncyI6Ijs7O0FBQUEscUNBQTBDO0FBQzFDLGtFQUFvRjtBQUNwRiw0RUFBOEY7QUFJOUYseUJBQXlCO0FBQ3pCLE1BQU0sVUFBVSxHQUFHLElBQUksb0NBQWdCLENBQUMsRUFBRSxDQUFDLENBQUM7QUFDNUMsTUFBTSxjQUFjLEdBQUcsSUFBSSw2Q0FBb0IsQ0FBQyxFQUFFLENBQUMsQ0FBQztBQUVwRCxLQUFLLFVBQVUsY0FBYyxDQUMzQixLQUFZLEVBQ1osYUFBcUIsRUFDckIsS0FBYTtJQUViLE1BQU0sS0FBSyxHQUFHLEtBQUssQ0FBQyxLQUFLLEVBQUUsQ0FBQztJQUU1QixJQUFJLENBQUM7UUFDSCxNQUFNLEtBQUssQ0FBQyxPQUFPLEVBQUUsQ0FBQztRQUV0Qix1QkFBdUI7UUFDdkIsTUFBTSxRQUFRLEdBQUcsTUFBTSxLQUFLLENBQUMsa0JBQWtCLENBQUMsRUFBRSxNQUFNLEVBQUUsQ0FBQyxLQUFLLENBQUMsRUFBRSxDQUFDLENBQUM7UUFDckUsTUFBTSxhQUFhLEdBQUcsUUFBUSxDQUFDLE1BQU0sQ0FBQyxJQUFJLENBQUMsQ0FBQyxDQUFDLEVBQUUsQ0FBQyxDQUFDLENBQUMsSUFBSSxLQUFLLEtBQUssQ0FBQyxDQUFDO1FBRWxFLElBQUksQ0FBQyxhQUFhLElBQUksYUFBYSxDQUFDLFVBQVUsQ0FBQyxNQUFNLEtBQUssQ0FBQyxFQUFFLENBQUM7WUFDNUQsT0FBTyxDQUFDLElBQUksQ0FBQyxpQ0FBaUMsS0FBSyxFQUFFLENBQUMsQ0FBQztZQUN2RCxPQUFPLElBQUksQ0FBQztRQUNkLENBQUM7UUFFRCxNQUFNLFVBQVUsR0FBRyxhQUFhLENBQUMsVUFBVSxDQUFDLEdBQUcsQ0FBQyxDQUFDLENBQUMsRUFBRSxDQUFDLENBQUMsQ0FBQyxXQUFXLENBQUMsQ0FBQztRQUVwRSxrQkFBa0I7UUFDbEIsTUFBTSxVQUFV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UUFDSCxDQUFDO1FBRUQsT0FBTztZQUNMLFVBQVUsRUFBRSxHQUFHO1lBQ2YsSUFBSSxFQUFFLElBQUksQ0FBQyxTQUFTLENBQUM7Z0JBQ25CLE9BQU8sRUFBRSxrQ0FBa0M7Z0JBQzNDLE9BQU87YUFDUixDQUFDO1NBQ0gsQ0FBQztJQUVKLENBQUM7SUFBQyxPQUFPLEtBQUssRUFBRSxDQUFDO1FBQ2YsT0FBTyxDQUFDLEtBQUssQ0FBQywwQkFBMEIsRUFBRSxLQUFLLENBQUMsQ0FBQztRQUNqRCxPQUFPO1lBQ0wsVUFBVSxFQUFFLEdBQUc7WUFDZixJQUFJLEVBQUUsSUFBSSxDQUFDLFNBQVMsQ0FBQztnQkFDbkIsS0FBSyxFQUFFLEtBQUssWUFBWSxLQUFLLENBQUMsQ0FBQyxDQUFDLEtBQUssQ0FBQyxPQUFPLENBQUMsQ0FBQyxDQUFDLGVBQWU7YUFDaEUsQ0FBQztTQUNILENBQUM7SUFDSixDQUFDO0FBQ0gsQ0FBQyxDQUFDO0FBM0VXLFFBQUEsT0FBTyxXQTJFbEIiLCJzb3VyY2VzQ29udGVudCI6WyJpbXBvcnQgeyBLYWZrYSwgbG9nTGV2ZWwgfSBmcm9tICdrYWZrYWpzJztcclxuaW1wb3J0IHsgQ2xvdWRXYXRjaENsaWVudCwgUHV0TWV0cmljRGF0YUNvbW1hbmQgfSBmcm9tICdAYXdzLXNkay9jbGllbnQtY2xvdWR3YXRjaCc7XHJcbmltcG9ydCB7IFNlY3JldHNNYW5hZ2VyQ2xpZW50LCBHZXRTZWNyZXRWYWx1ZUNvbW1hbmQgfSBmcm9tICdAYXdzLXNkay9jbGllbnQtc2VjcmV0cy1tYW5hZ2VyJztcclxuaW1wb3J0IHsgQ29udGV4dCwgSGFuZGxlciB9IGZyb20gJ2F3cy1sYW1iZGEnO1xyXG5pbXBvcnQgeyBDb25zdW1lckNvbmZpZywgU1NMQ2VydGlmaWNhdGVzLCBLYWZrYUNvbmZpZywgQ29uc3VtZXJMYWdSZXN1bHQsIExhbWJkYVJlc3BvbnNlIH0gZnJvbSAnLi90eXBlcyc7XHJcblxyXG4vLyBJbml0aWFsaXplIEFXUyBjbGllbnRzXHJcbmNvbnN0IGNsb3VkV2F0Y2ggPSBuZXcgQ2xvdWRXYXRjaENsaWVudCh7fSk7XHJcbmNvbnN0IHNlY3JldHNNYW5hZ2VyID0gbmV3IFNlY3JldHNNYW5hZ2VyQ2xpZW50KHt9KTtcclxuXHJcbmFzeW5jIGZ1bmN0aW9uIGdldENvbnN1bWVyTGFnKFxyXG4gIGthZmthOiBLYWZrYSxcclxuICBjb25zdW1lckdyb3VwOiBzdHJpbmcsXHJcbiAgdG9waWM6IHN0cmluZ1xyXG4pOiBQcm9taXNlPG51bWJlciB8IG51bGw+IHtcclxuICBjb25zdCBhZG1pbiA9IGthZmthLmFkbWluKCk7XHJcbiAgXHJcbiAgdHJ5IHtcclxuICAgIGF3YWl0IGFkbWluLmNvbm5lY3QoKTtcclxuICAgIFxyXG4gICAgLy8gR2V0IHRvcGljIHBhcnRpdGlvbnNcclxuICAgIGNvbnN0IG1ldGFkYXRhID0gYXdhaXQgYWRtaW4uZmV0Y2hUb3BpY01ldGFkYXRhKHsgdG9waWNzOiBbdG9waWNdIH0pO1xyXG4gICAgY29uc3QgdG9waWNNZXRhZGF0YSA9IG1ldGFkYXRhLnRvcGljcy5maW5kKHQgPT4gdC5uYW1lID09PSB0b3BpYyk7XHJcbiAgICBcclxuICAgIGlmICghdG9waWNNZXRhZGF0YSB8fCB0b3BpY01ldGFkYXRhLnBhcnRpdGlvbnMubGVuZ3RoID09PSAwKSB7XHJcbiAgICAgIGNvbnNvbGUud2FybihgTm8gcGFydGl0aW9ucyBmb3VuZCBmb3IgdG9waWMgJHt0b3BpY31gKTtcclxuICAgICAgcmV0dXJuIG51bGw7XHJcbiAgICB9XHJcbiAgICBcclxuICAgIGNvbnN0IHBhcnRpdGlvbnMgPSB0b3BpY01ldGFkYXRhLnBhcnRpdGlvbnMubWFwKHAgPT4gcC5wYXJ0aXRpb25JZCk7XHJcbiAgICBcclxuICAgIC8vIEdldCBlbmQgb2Zmc2V0c1xyXG4gICAgY29uc3QgZW5kT2Zmc2V0cyA9IGF3YWl0IGFkbWluLmZldGNoVG9waWNPZmZzZXRzKHRvcGljKTtcclxuICAgIGlmICghZW5kT2Zmc2V0cyB8fCBlbmRPZmZzZXRzLmxlbmd0aCA9PT0gMCkge1xyXG4gICAgICBjb25zb2xlLndhcm4oYE5vIGVuZCBvZmZzZXRzIGZvdW5kIGZvciB0b3BpYyAke3RvcGljfWApO1xyXG4gICAgICByZXR1cm4gbnVsbDtcclxuICAgIH1cclxuICAgIFxyXG4gICAgLy8gR2V0IGNvbnN1bWVyIGdyb3VwIG9mZnNldHNcclxuICAgIGNvbnN0IGdyb3VwT2Zmc2V0cyA9IGF3YWl0IGFkbWluLmZldGNoT2Zmc2V0cyh7IGdyb3VwSWQ6IGNvbnN1bWVyR3JvdXAsIHRvcGljczogW3RvcGljXSB9KTtcclxuICAgIGlmICghZ3JvdXBPZmZzZXRzIHx8IGdyb3VwT2Zmc2V0cy5sZW5ndGggPT09IDApIHtcclxuICAgICAgY29uc29sZS53YXJuKGBObyBvZmZzZXRzIGZvdW5kIGZvciBjb25zdW1lciBncm91cCAke2NvbnN1bWVyR3JvdXB9IG9uIHRvcGljICR7dG9waWN9YCk7XHJcbiAgICAgIHJldHVybiBudWxsO1xyXG4gICAgfVxyXG4gICAgXHJcbiAgICAvLyBDYWxjdWxhdGUgdG90YWwgbGFnXHJcbiAgICBsZXQgdG90YWxMYWcgPSAwO1xyXG4gICAgY29uc3QgY29uc3VtZXJQYXJ0aXRpb25zID0gZ3JvdXBPZmZzZXRzWzBdLnBhcnRpdGlvbnM7XHJcbiAgICBcclxuICAgIGZvciAoY29uc3QgcGFydGl0aW9uIG9mIHBhcnRpdGlvbnMpIHtcclxuICAgICAgY29uc3QgZW5kT2Zmc2V0ID0gZW5kT2Zmc2V0cy5maW5kKG8gPT4gby5wYXJ0aXRpb24gPT09IHBhcnRpdGlvbik/Lm9mZnNldCB8fCAnMCc7XHJcbiAgICAgIGNvbnN0IGNvbnN1bWVyT2Zmc2V0ID0gY29uc3VtZXJQYXJ0aXRpb25zLmZpbmQocCA9PiBwLnBhcnRpdGlvbiA9PT0gcGFydGl0aW9uKT8ub2Zmc2V0IHx8ICcwJztcclxuICAgICAgXHJcbiAgICAgIGNvbnN0IGxhZyA9IHBhcnNlSW50KGVuZE9mZnNldCkgLSBwYXJzZUludChjb25zdW1lck9mZnNldCk7XHJcbiAgICAgIGlmIChsYWcgPiAwKSB7XHJcbiAgICAgICAgdG90YWxMYWcgKz0gbGFnO1xyXG4gICAgICB9XHJcbiAgICB9XHJcbiAgICBcclxuICAgIGNvbnNvbGUuaW5mbyhgQ2FsY3VsYXRlZCBsYWcgZm9yICR7Y29uc3VtZXJHcm91cH0gb24gJHt0b3BpY306ICR7dG90YWxMYWd9IG1lc3NhZ2VzIGFjcm9zcyAke3BhcnRpdGlvbnMubGVuZ3RofSBwYXJ0aXRpb25zYCk7XHJcbiAgICByZXR1cm4gdG90YWxMYWc7XHJcbiAgICBcclxuICB9IGNhdGNoIChlcnJvcikge1xyXG4gICAgY29uc29sZS5lcnJvcihgRXJyb3IgY29sbGVjdGluZyBsYWcgZm9yICR7Y29uc3VtZXJHcm91cH0gb24gJHt0b3BpY306YCwgZXJyb3IpO1xyXG4gICAgcmV0dXJuIG51bGw7XHJcbiAgfSBmaW5hbGx5IHtcclxuICAgIGF3YWl0IGFkbWluLmRpc2Nvbm5lY3QoKTtcclxuICB9XHJcbn1cclxuXHJcbmFzeW5jIGZ1bmN0aW9uIHB1Ymxpc2hNZXRyaWMoXHJcbiAgY29uc3VtZXJHcm91cDogc3RyaW5nLFxyXG4gIHRvcGljOiBzdHJpbmcsXHJcbiAgbGFnVmFsdWU6IG51bWJlcixcclxuICBuYW1lc3BhY2U6IHN0cmluZ1xyXG4pOiBQcm9taXNlPHZvaWQ+IHtcclxuICB0cnkge1xyXG4gICAgY29uc3QgY29tbWFuZCA9IG5ldyBQdXRNZXRyaWNEYXRhQ29tbWFuZCh7XHJcbiAgICAgIE5hbWVzcGFjZTogbmFtZXNwYWNlLFxyXG4gICAgICBNZXRyaWNEYXRhOiBbe1xyXG4gICAgICAgIE1ldHJpY05hbWU6ICdjdXN0b20ua2Fma2EuY29uc3VtZXIubGFnJyxcclxuICAgICAgICBWYWx1ZTogbGFnVmFsdWUsXHJcbiAgICAgICAgVW5pdDogJ0NvdW50JyxcclxuICAgICAgICBEaW1lbnNpb25zOiBbXHJcbiAgICAgICAgICB7IE5hbWU6ICdjb25zdW1lcl9ncm91cCcsIFZhbHVlOiBjb25zdW1lckdyb3VwIH0sXHJcbiAgICAgICAgICB7IE5hbWU6ICd0b3BpYycsIFZhbHVlOiB0b3BpYyB9LFxyXG4gICAgICAgICAgeyBOYW1lOiAnbWV0cmljX3R5cGUnLCBWYWx1ZTogJ2NvbnN1bWVyX2xhZycgfVxyXG4gICAgICAgIF0sXHJcbiAgICAgICAgU3RvcmFnZVJlc29sdXRpb246IDYwIC8vIDEtbWludXRlIHJlc29sdXRpb25cclxuICAgICAgfV1cclxuICAgIH0pO1xyXG4gICAgXHJcbiAgICBhd2FpdCBjbG91ZFdhdGNoLnNlbmQoY29tbWFuZCk7XHJcbiAgICBjb25zb2xlLmluZm8oYFB1Ymxpc2hlZCBsYWcgbWV0cmljOiBncm91cD0ke2NvbnN1bWVyR3JvdXB9LCB0b3BpYz0ke3RvcGljfSwgbGFnPSR7bGFnVmFsdWV9YCk7XHJcbiAgfSBjYXRjaCAoZXJyb3IpIHtcclxuICAgIGNvbnNvbGUuZXJyb3IoJ0Vycm9yIHB1Ymxpc2hpbmcgbWV0cmljIHRvIENsb3VkV2F0Y2g6JywgZXJyb3IpO1xyXG4gICAgdGhyb3cgZXJyb3I7XHJcbiAgfVxyXG59XHJcblxyXG5hc3luYyBmdW5jdGlvbiBnZXRTU0xDb25maWcoKTogUHJvbWlzZTxLYWZrYUNvbmZpZ1snc3NsJ10gfCB1bmRlZmluZWQ+IHtcclxuICBpZiAocHJvY2Vzcy5lbnYuS0FGS0FfU1NMX0VOQUJMRUQgIT09ICd0cnVlJykge1xyXG4gICAgcmV0dXJuIHVuZGVmaW5lZDtcclxuICB9XHJcblxyXG4gIHRyeSB7XHJcbiAgICBsZXQgY2VydHM6IFNTTENlcnRpZmljYXRlcztcclxuICAgIFxyXG4gICAgaWYgKHByb2Nlc3MuZW52LktBRktBX0NFUlRJRklDQVRFU19TRUNSRVRfQVJOKSB7XHJcbiAgICAgIGNvbnN0IGNvbW1hbmQgPSBuZXcgR2V0U2VjcmV0VmFsdWVDb21tYW5kKHtcclxuICAgICAgICBTZWNyZXRJZDogcHJvY2Vzcy5lbnYuS0FGS0FfQ0VSVElGSUNBVEVTX1NFQ1JFVF9BUk5cclxuICAgICAgfSk7XHJcbiAgICAgIFxyXG4gICAgICBjb25zdCByZXNwb25zZSA9IGF3YWl0IHNlY3JldHNNYW5hZ2VyLnNlbmQoY29tbWFuZCk7XHJcbiAgICAgIGNlcnRzID0gSlNPTi5wYXJzZShyZXNwb25zZS5TZWNyZXRTdHJpbmcgfHwgJ3t9Jyk7XHJcbiAgICB9IGVsc2Uge1xyXG4gICAgICAvLyBVc2UgZGVmYXVsdCBjZXJ0aWZpY2F0ZSBwYXRoc1xyXG4gICAgICBjZXJ0cyA9IHtcclxuICAgICAgICBjYTogJy9vcHQva2Fma2EvY2VydC9jYS5jcnQnLFxyXG4gICAgICAgIGNlcnQ6ICcvb3B0L2thZmthL2NlcnQvY2xpZW50LmNydCcsXHJcbiAgICAgICAga2V5OiAnL29wdC9rYWZrYS9jZXJ0L2NsaWVudC5rZXknXHJcbiAgICAgIH07XHJcbiAgICB9XHJcbiAgICBcclxuICAgIHJldHVybiB7XHJcbiAgICAgIGNhOiBbY2VydHMuY2FdLFxyXG4gICAgICBrZXk6IGNlcnRzLmtleSxcclxuICAgICAgY2VydDogY2VydHMuY2VydCxcclxuICAgICAgcmVqZWN0VW5hdXRob3JpemVkOiB0cnVlXHJcbiAgICB9O1xyXG4gIH0gY2F0Y2ggKGVycm9yKSB7XHJcbiAgICBjb25zb2xlLmVycm9yKCdFcnJvciBnZXR0aW5nIFNTTCBjb25maWd1cmF0aW9uOicsIGVycm9yKTtcclxuICAgIHRocm93IGVycm9yO1xyXG4gIH1cclxufVxyXG5cclxuZXhwb3J0IGNvbnN0IGhhbmRsZXI6IEhhbmRsZXIgPSBhc3luYyAoZXZlbnQ6IGFueSwgY29udGV4dDogQ29udGV4dCk6IFByb21pc2U8TGFtYmRhUmVzcG9uc2U+ID0+IHtcclxuICB0cnkge1xyXG4gICAgLy8gR2V0IGNvbmZpZ3VyYXRpb24gZnJvbSBlbnZpcm9ubWVudCB2YXJpYWJsZXNcclxuICAgIGNvbnN0IGJvb3RzdHJhcFNlcnZlcnMgPSBwcm9jZXNzLmVudi5LQUZLQV9CT09UU1RSQVBfU0VSVkVSUztcclxuICAgIGNvbnN0IG5hbWVzcGFjZSA9IHByb2Nlc3MuZW52LkNMT1VEV0FUQ0hfTkFNRVNQQUNFO1xyXG4gICAgXHJcbiAgICBpZiAoIWJvb3RzdHJhcFNlcnZlcnMgfHwgIW5hbWVzcGFjZSkge1xyXG4gICAgICB0aHJvdyBuZXcgRXJyb3IoJ1JlcXVpcmVkIGVudmlyb25tZW50IHZhcmlhYmxlcyBhcmUgbm90IHNldCcpO1xyXG4gICAgfVxyXG4gICAgXHJcbiAgICAvLyBHZXQgU1NMIGNvbmZpZ3VyYXRpb24gaWYgZW5hYmxlZFxyXG4gICAgY29uc3Qgc3NsID0gYXdhaXQgZ2V0U1NMQ29uZmlnKCk7XHJcbiAgICBcclxuICAgIC8vIENvbmZpZ3VyZSBLYWZrYSBjbGllbnRcclxuICAgIGNvbnN0IGthZmthID0gbmV3IEthZmthKHtcclxuICAgICAgYnJva2VyczogYm9vdHN0cmFwU2VydmVycy5zcGxpdCgnLCcpLFxyXG4gICAgICBjbGllbnRJZDogJ2xhZy1jb2xsZWN0b3InLFxyXG4gICAgICBzc2wsXHJcbiAgICAgIGxvZ0xldmVsOiBsb2dMZXZlbC5JTkZPXHJcbiAgICB9KTtcclxuICAgIFxyXG4gICAgLy8gR2V0IGNvbnN1bWVyIGdyb3VwcyBhbmQgdG9waWNzIGZyb20gZW52aXJvbm1lbnRcclxuICAgIGNvbnN0IGNvbnN1bWVyc0NvbmZpZzogQ29uc3VtZXJDb25maWdbXSA9IEpTT04ucGFyc2UocHJvY2Vzcy5lbnYuQ09OU1VNRVJfQ09ORklHIHx8ICdbXScpO1xyXG4gICAgXHJcbiAgICBjb25zdCByZXN1bHRzOiBDb25zdW1lckxhZ1Jlc3VsdFtdID0gW107XHJcbiAgICBmb3IgKGNvbnN0IGNvbmZpZyBvZiBjb25zdW1lcnNDb25maWcpIHtcclxuICAgICAgdHJ5IHtcclxuICAgICAgICAvLyBHZXQgbGFnIGZvciB0aGlzIGNvbnN1bWVyIGdyb3VwIGFuZCB0b3BpY1xyXG4gICAgICAgIGNvbnN0IGxhZyA9IGF3YWl0IGdldENvbnN1bWVyTGFnKGthZmthLCBjb25maWcuY29uc3VtZXJfZ3JvdXAsIGNvbmZpZy50b3BpYyk7XHJcbiAgICAgICAgXHJcbiAgICAgICAgaWYgKGxhZyAhPT0gbnVsbCkge1xyXG4gICAgICAgICAgLy8gUHVibGlzaCB0byBDbG91ZFdhdGNoXHJcbiAgICAgICAgICBhd2FpdCBwdWJsaXNoTWV0cmljKGNvbmZpZy5jb25zdW1lcl9ncm91cCwgY29uZmlnLnRvcGljLCBsYWcsIG5hbWVzcGFjZSk7XHJcbiAgICAgICAgICByZXN1bHRzLnB1c2goe1xyXG4gICAgICAgICAgICBjb25zdW1lcl9ncm91cDogY29uZmlnLmNvbnN1bWVyX2dyb3VwLFxyXG4gICAgICAgICAgICB0b3BpYzogY29uZmlnLnRvcGljLFxyXG4gICAgICAgICAgICBsYWcsXHJcbiAgICAgICAgICAgIHN0YXR1czogJ3N1Y2Nlc3MnXHJcbiAgICAgICAgICB9KTtcclxuICAgICAgICB9IGVsc2Uge1xyXG4gICAgICAgICAgcmVzdWx0cy5wdXNoKHtcclxuICAgICAgICAgICAgY29uc3VtZXJfZ3JvdXA6IGNvbmZpZy5jb25zdW1lcl9ncm91cCxcclxuICAgICAgICAgICAgdG9waWM6IGNvbmZpZy50b3BpYyxcclxuICAgICAgICAgICAgc3RhdHVzOiAnZXJyb3InLFxyXG4gICAgICAgICAgICBtZXNzYWdlOiAnRmFpbGVkIHRvIGNhbGN1bGF0ZSBsYWcnXHJcbiAgICAgICAgICB9KTtcclxuICAgICAgICB9XHJcbiAgICAgIH0gY2F0Y2ggKGVycm9yKSB7XHJcbiAgICAgICAgY29uc29sZS5lcnJvcihgRmFpbGVkIHRvIHByb2Nlc3MgJHtjb25maWcuY29uc3VtZXJfZ3JvdXB9IG9uICR7Y29uZmlnLnRvcGljfTpgLCBlcnJvcik7XHJcbiAgICAgICAgcmVzdWx0cy5wdXNoKHtcclxuICAgICAgICAgIGNvbnN1bWVyX2dyb3VwOiBjb25maWcuY29uc3VtZXJfZ3JvdXAsXHJcbiAgICAgICAgICB0b3BpYzogY29uZmlnLnRvcGljLFxyXG4gICAgICAgICAgc3RhdHVzOiAnZXJyb3InLFxyXG4gICAgICAgICAgbWVzc2FnZTogZXJyb3IgaW5zdGFuY2VvZiBFcnJvciA/IGVycm9yLm1lc3NhZ2UgOiAnVW5rbm93biBlcnJvcidcclxuICAgICAgICB9KTtcclxuICAgICAgfVxyXG4gICAgfVxyXG4gICAgXHJcbiAgICByZXR1cm4ge1xyXG4gICAgICBzdGF0dXNDb2RlOiAyMDAsXHJcbiAgICAgIGJvZHk6IEpTT04uc3RyaW5naWZ5KHtcclxuICAgICAgICBtZXNzYWdlOiAnTGFnIG1ldHJpY3MgY29sbGVjdGlvbiBjb21wbGV0ZWQnLFxyXG4gICAgICAgIHJlc3VsdHNcclxuICAgICAgfSlcclxuICAgIH07XHJcbiAgICBcclxuICB9IGNhdGNoIChlcnJvcikge1xyXG4gICAgY29uc29sZS5lcnJvcignTGFtYmRhIGV4ZWN1dGlvbiBmYWlsZWQ6JywgZXJyb3IpO1xyXG4gICAgcmV0dXJuIHtcclxuICAgICAgc3RhdHVzQ29kZTogNTAwLFxyXG4gICAgICBib2R5OiBKU09OLnN0cmluZ2lmeSh7XHJcbiAgICAgICAgZXJyb3I6IGVycm9yIGluc3RhbmNlb2YgRXJyb3IgPyBlcnJvci5tZXNzYWdlIDogJ1Vua25vd24gZXJyb3InXHJcbiAgICAgIH0pXHJcbiAgICB9O1xyXG4gIH1cclxufTsgIl19