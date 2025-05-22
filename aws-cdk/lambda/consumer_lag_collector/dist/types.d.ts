import { SASLOptions } from 'kafkajs';
import { ConnectionOptions } from 'tls';
export interface ConsumerConfig {
    consumer_group: string;
    topic: string;
}
export interface SSLCertificates {
    ca: string;
    cert: string;
    key: string;
}
export interface KafkaConfig {
    brokers: string[];
    clientId: string;
    ssl?: ConnectionOptions;
    sasl?: SASLOptions;
    logLevel?: number;
}
export interface ConsumerLagResult {
    consumer_group: string;
    topic: string;
    lag?: number;
    status: 'success' | 'error';
    message?: string;
}
export interface LambdaResponse {
    statusCode: number;
    body: string;
}
