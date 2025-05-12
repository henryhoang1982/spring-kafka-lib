import * as cdk from 'aws-cdk-lib';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as ecs from 'aws-cdk-lib/aws-ecs';
import * as elbv2 from 'aws-cdk-lib/aws-elasticloadbalancingv2';
import * as appscaling from 'aws-cdk-lib/aws-applicationautoscaling';
import * as cloudwatch from 'aws-cdk-lib/aws-cloudwatch';
import { Construct } from 'constructs';
import { ScalableTaskCount } from 'aws-cdk-lib/aws-ecs';

export interface BaseServiceStackProps extends cdk.StackProps {
  vpc: ec2.IVpc;
  cluster: ecs.ICluster;
  serviceName: string;
  containerPort: number;
  cpu: number;
  memoryLimitMiB: number;
  desiredCount: number;
  minCapacity: number;
  maxCapacity: number;
  healthCheckPath: string;
  imageUri: string;
}

export interface ScalingStep {
  lower: number;
  upper?: number;
  capacity: number;
}

export abstract class BaseServiceStack extends cdk.Stack {
  protected service: ecs.FargateService;
  protected taskDefinition: ecs.FargateTaskDefinition;
  protected loadBalancer: elbv2.ApplicationLoadBalancer;
  protected targetGroup: elbv2.ApplicationTargetGroup;

  constructor(scope: Construct, id: string, props: BaseServiceStackProps) {
    super(scope, id, props);

    // Create Task Definition
    this.taskDefinition = new ecs.FargateTaskDefinition(this, 'TaskDef', {
      memoryLimitMiB: props.memoryLimitMiB,
      cpu: props.cpu,
    });

    // Add container to task definition
    const container = this.taskDefinition.addContainer('AppContainer', {
      image: ecs.ContainerImage.fromRegistry(props.imageUri),
      memoryLimitMiB: props.memoryLimitMiB,
      logging: ecs.LogDrivers.awsLogs({ streamPrefix: props.serviceName }),
      portMappings: [{ containerPort: props.containerPort }],
    });

    // Create ALB
    this.loadBalancer = new elbv2.ApplicationLoadBalancer(this, 'ALB', {
      vpc: props.vpc,
      internetFacing: true,
    });

    // Create target group
    this.targetGroup = new elbv2.ApplicationTargetGroup(this, 'TargetGroup', {
      vpc: props.vpc,
      port: props.containerPort,
      protocol: elbv2.ApplicationProtocol.HTTP,
      targetType: elbv2.TargetType.IP,
      healthCheck: {
        path: props.healthCheckPath,
        unhealthyThresholdCount: 2,
        healthyThresholdCount: 5,
        interval: cdk.Duration.seconds(30),
      },
    });

    // Add listener
    this.loadBalancer.addListener('Listener', {
      port: 80,
      defaultTargetGroups: [this.targetGroup],
    });

    // Create Fargate Service
    this.service = new ecs.FargateService(this, 'Service', {
      cluster: props.cluster,
      taskDefinition: this.taskDefinition,
      desiredCount: props.desiredCount,
      assignPublicIp: true,
      serviceName: props.serviceName,
    });

    // Add target group to service
    this.service.attachToApplicationTargetGroup(this.targetGroup);

    // Setup autoscaling
    const scaling = this.service.autoScaleTaskCount({
      minCapacity: props.minCapacity,
      maxCapacity: props.maxCapacity,
    });

    // Allow subclasses to add custom scaling rules
    this.configureScaling(scaling);
  }

  protected abstract configureScaling(scaling: ecs.ScalableTaskCount): void;

  protected createTotalLagScaling(
    scaling: ecs.ScalableTaskCount,
    metricName: string,
    namespace: string,
    dimensions: { [key: string]: string },
    steps: ScalingStep[]
  ): void {
    const metric = new cloudwatch.Metric({
      metricName: metricName,
      namespace: namespace,
      dimensionsMap: dimensions,
      period: cdk.Duration.minutes(1),
      statistic: 'Maximum',
    });

    // Create step scaling policy
    const scalingPolicy = new appscaling.StepScalingPolicy(this, 'StepScaling', {
      scalingTarget: scaling as unknown as appscaling.IScalableTarget,
      metric: metric,
      scalingSteps: steps.map(step => ({
        lower: step.lower,
        upper: step.upper,
        change: step.capacity,
      })),
      adjustmentType: appscaling.AdjustmentType.EXACT_CAPACITY,
    });
  }
} 