import { Kafka, logLevel } from 'kafkajs';
import { CloudWatchClient, PutMetricDataCommand } from '@aws-sdk/client-cloudwatch';
import { SecretsManagerClient, GetSecretValueCommand } from '@aws-sdk/client-secrets-manager';
import { Context, Handler } from 'aws-lambda';
import { ConsumerConfig, SSLCertificates, KafkaConfig, ConsumerLagResult, LambdaResponse } from './types';

// Initialize AWS clients
const cloudWatch = new CloudWatchClient({});
const secretsManager = new SecretsManagerClient({});

async function getConsumerLag(
  kafka: Kafka,
  consumerGroup: string,
  topic: string
): Promise<number | null> {
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
    
  } catch (error) {
    console.error(`Error collecting lag for ${consumerGroup} on ${topic}:`, error);
    return null;
  } finally {
    await admin.disconnect();
  }
}

async function publishMetric(
  consumerGroup: string,
  topic: string,
  lagValue: number,
  namespace: string
): Promise<void> {
  try {
    const command = new PutMetricDataCommand({
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
  } catch (error) {
    console.error('Error publishing metric to CloudWatch:', error);
    throw error;
  }
}

async function getSSLConfig(): Promise<KafkaConfig['ssl'] | undefined> {
  if (process.env.KAFKA_SSL_ENABLED !== 'true') {
    return undefined;
  }

  try {
    let certs: SSLCertificates;
    
    if (process.env.KAFKA_CERTIFICATES_SECRET_ARN) {
      const command = new GetSecretValueCommand({
        SecretId: process.env.KAFKA_CERTIFICATES_SECRET_ARN
      });
      
      const response = await secretsManager.send(command);
      certs = JSON.parse(response.SecretString || '{}');
    } else {
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
  } catch (error) {
    console.error('Error getting SSL configuration:', error);
    throw error;
  }
}

export const handler: Handler = async (event: any, context: Context): Promise<LambdaResponse> => {
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
    const kafka = new Kafka({
      brokers: bootstrapServers.split(','),
      clientId: 'lag-collector',
      ssl,
      logLevel: logLevel.INFO
    });
    
    // Get consumer groups and topics from environment
    const consumersConfig: ConsumerConfig[] = JSON.parse(process.env.CONSUMER_CONFIG || '[]');
    
    const results: ConsumerLagResult[] = [];
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
        } else {
          results.push({
            consumer_group: config.consumer_group,
            topic: config.topic,
            status: 'error',
            message: 'Failed to calculate lag'
          });
        }
      } catch (error) {
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
    
  } catch (error) {
    console.error('Lambda execution failed:', error);
    return {
      statusCode: 500,
      body: JSON.stringify({
        error: error instanceof Error ? error.message : 'Unknown error'
      })
    };
  }
}; 