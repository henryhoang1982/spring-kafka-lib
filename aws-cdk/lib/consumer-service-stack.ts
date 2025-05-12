import * as servicescaling from 'aws-cdk-lib/aws-applicationautoscaling';
import { BaseServiceStack, BaseServiceStackProps } from './base-service-stack';
import { Construct } from 'constructs';
import * as config from '../config/service-config.json';
import { ScalableTaskCount } from 'aws-cdk-lib/aws-ecs';

export interface ConsumerServiceStackProps extends BaseServiceStackProps {
  scaling: {
    metric: string;
    namespace: string;
    dimensions: { [key: string]: string };
    steps: Array<{
      lower: number;
      upper?: number;
      capacity: number;
    }>;
  };
}

export class ConsumerServiceStack extends BaseServiceStack {
  private readonly serviceProps: ConsumerServiceStackProps;

  constructor(scope: Construct, id: string, props: ConsumerServiceStackProps) {
    super(scope, id, props);
    this.serviceProps = props;
  }

  protected configureScaling(scaling: ScalableTaskCount): void {
    // Use the protected method from base class to setup lag-based scaling
    this.createTotalLagScaling(
      scaling,
      this.serviceProps.scaling.metric,
      this.serviceProps.scaling.namespace,
      this.serviceProps.scaling.dimensions,
      this.serviceProps.scaling.steps
    );
  }
} 