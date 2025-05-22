import * as cdk from 'aws-cdk-lib';
import * as lambda from 'aws-cdk-lib/aws-lambda';
import * as iam from 'aws-cdk-lib/aws-iam';
import * as events from 'aws-cdk-lib/aws-events';
import * as targets from 'aws-cdk-lib/aws-events-targets';
import * as secretsmanager from 'aws-cdk-lib/aws-secretsmanager';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import { Construct } from 'constructs';
import * as path from 'path';

export interface ConsumerLagCollectorStackProps extends cdk.StackProps {
  kafkaBootstrapServers: string;
  cloudwatchNamespace: string;
  consumerConfig: Array<{
    consumer_group: string;
    topic: string;
  }>;
  sslEnabled?: boolean;
  kafkaCertificatesSecretArn?: string;
  vpc?: ec2.IVpc;  // Optional VPC for private Kafka access
  vpcSubnets?: ec2.SubnetSelection;  // Optional subnet selection
}

export class ConsumerLagCollectorStack extends cdk.Stack {
  constructor(scope: Construct, id: string, props: ConsumerLagCollectorStackProps) {
    super(scope, id, props);

    // Create Lambda layer for Kafka dependencies
    const kafkaLayer = new lambda.LayerVersion(this, 'KafkaLayer', {
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/consumer_lag_collector'), {
        bundling: {
          image: lambda.Runtime.NODEJS_18_X.bundlingImage,
          command: [
            'bash', '-c', [
              'npm install',
              'npm run build',
              'cp -r node_modules dist/',
              'cp package.json dist/',
              'cd dist && zip -r /asset-output/layer.zip .'
            ].join(' && ')
          ],
          user: 'root'
        }
      }),
      compatibleRuntimes: [lambda.Runtime.NODEJS_18_X],
      description: 'Kafka dependencies layer',
    });

    // Lambda function configuration
    const functionProps: lambda.FunctionProps = {
      runtime: lambda.Runtime.NODEJS_18_X,
      code: lambda.Code.fromAsset(path.join(__dirname, '../lambda/consumer_lag_collector'), {
        bundling: {
          image: lambda.Runtime.NODEJS_18_X.bundlingImage,
          command: [
            'bash', '-c', [
              'npm install',
              'npm run build',
              'cp package.json dist/',
              'cd dist && zip -r /asset-output/function.zip .'
            ].join(' && ')
          ],
          user: 'root'
        }
      }),
      handler: 'index.handler',
      timeout: cdk.Duration.seconds(30),
      memorySize: 512,  // Increased memory for better performance
      environment: {
        KAFKA_BOOTSTRAP_SERVERS: props.kafkaBootstrapServers,
        CLOUDWATCH_NAMESPACE: props.cloudwatchNamespace,
        CONSUMER_CONFIG: JSON.stringify(props.consumerConfig),
        KAFKA_SSL_ENABLED: props.sslEnabled ? 'true' : 'false',
        POWERTOOLS_SERVICE_NAME: 'consumer-lag-collector',
        POWERTOOLS_METRICS_NAMESPACE: props.cloudwatchNamespace,
        NODE_OPTIONS: '--enable-source-maps'
      },
      layers: [kafkaLayer],
      // Enable X-Ray tracing
      tracing: lambda.Tracing.ACTIVE,
      // Configure log retention
      logRetention: cdk.aws_logs.RetentionDays.ONE_WEEK,
      // Configure reserved concurrent executions to prevent overload
      reservedConcurrentExecutions: 1,
      // Add VPC configuration if provided
      ...(props.vpc && {
        vpc: props.vpc,
        vpcSubnets: props.vpcSubnets || {
          subnetType: ec2.SubnetType.PRIVATE_WITH_EGRESS
        }
      })
    };

    // Add VPC endpoints if needed
    if (props.vpc) {
      new ec2.InterfaceVpcEndpoint(this, 'CloudWatchEndpoint', {
        vpc: props.vpc,
        service: ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH,
        subnets: props.vpcSubnets,
      });

      new ec2.InterfaceVpcEndpoint(this, 'SecretsManagerEndpoint', {
        vpc: props.vpc,
        service: ec2.InterfaceVpcEndpointAwsService.SECRETS_MANAGER,
        subnets: props.vpcSubnets,
      });
    }

    // Create the Lambda function
    const lagCollectorLambda = new lambda.Function(this, 'ConsumerLagCollector', functionProps);

    // Add CloudWatch Metrics permissions
    lagCollectorLambda.addToRolePolicy(new iam.PolicyStatement({
      effect: iam.Effect.ALLOW,
      actions: ['cloudwatch:PutMetricData'],
      resources: ['*']
    }));

    // If SSL is enabled, add permissions to access certificates
    if (props.sslEnabled && props.kafkaCertificatesSecretArn) {
      const certificatesSecret = secretsmanager.Secret.fromSecretCompleteArn(
        this,
        'KafkaCertificates',
        props.kafkaCertificatesSecretArn
      );

      certificatesSecret.grantRead(lagCollectorLambda);
      
      lagCollectorLambda.addEnvironment(
        'KAFKA_CERTIFICATES_SECRET_ARN',
        props.kafkaCertificatesSecretArn
      );
    }

    // Create EventBridge rule to trigger Lambda every minute
    new events.Rule(this, 'ScheduleRule', {
      schedule: events.Schedule.rate(cdk.Duration.minutes(1)),
      targets: [new targets.LambdaFunction(lagCollectorLambda, {
        // Retry policy for failed executions
        retryAttempts: 2,
        maxEventAge: cdk.Duration.minutes(5)
      })]
    });

    // Add custom CloudWatch dashboard
    const dashboard = new cdk.aws_cloudwatch.Dashboard(this, 'LagMetricsDashboard', {
      dashboardName: 'KafkaConsumerLagMetrics'
    });

    // Add widgets for each consumer group
    props.consumerConfig.forEach(config => {
      dashboard.addWidgets(
        new cdk.aws_cloudwatch.GraphWidget({
          title: `Consumer Lag - ${config.consumer_group}`,
          left: [
            new cdk.aws_cloudwatch.Metric({
              namespace: props.cloudwatchNamespace,
              metricName: 'custom.kafka.consumer.lag',
              dimensionsMap: {
                consumer_group: config.consumer_group,
                topic: config.topic,
                metric_type: 'consumer_lag'
              },
              statistic: 'Maximum',
              period: cdk.Duration.minutes(1)
            })
          ]
        })
      );
    });
  }
} 