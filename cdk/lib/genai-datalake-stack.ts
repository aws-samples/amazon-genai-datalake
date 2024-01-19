import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import { aws_emr as emr } from 'aws-cdk-lib';
import { aws_emrserverless as emrserverless } from 'aws-cdk-lib';
import { aws_sagemaker as sagemaker } from 'aws-cdk-lib';
import { aws_athena as athena } from 'aws-cdk-lib';

export class GenAIDataLake extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // VPC for EMR and SageMaker Studios
    const vpc = new ec2.Vpc(this, 'Vpc', {
      maxAzs: 2
    });

    // S3 bucket for Studio storage
    const bucket = new s3.Bucket(this, 's3Bucket', {
      encryption: s3.BucketEncryption.KMS,
      bucketKeyEnabled: true,
      blockPublicAccess: s3.BlockPublicAccess.BLOCK_ALL,
    })

    //shared policies
    const sharedS3Policy = new iam.PolicyStatement({
      resources: [
        bucket.bucketArn,
        bucket.bucketArn + '/*'
      ],
      actions: [
        's3:Get*',
        "s3:DeleteObject",
        's3:List*',
        's3:Read*',
        's3:Put*',
        's3:Write*'
      ],
    });
    const sharedIAMpolicy = new iam.PolicyStatement({
      resources: [
        '*'
      ],
      actions: [
        "kms:Decrypt",
        "kms:GenerateDataKey",
        "kms:ReEncryptFrom",
        "kms:ReEncryptTo",
        "kms:DescribeKey",
        "kms:Encrypt",
      ],
    });

    // EMR Studio application role
    const appRole = new iam.Role(this, 'EMRStudioAppRole', {
      assumedBy: new iam.ServicePrincipal('emr-serverless.amazonaws.com'),
    });
    appRole.addToPolicy(new iam.PolicyStatement({
      resources: ['*'],
      actions: ['glue:*'],
    }));
    appRole.addToPolicy(sharedS3Policy)
    appRole.addToPolicy(sharedIAMpolicy)

    // EMR Studio role
    const studioRole = new iam.Role(this, 'EMRStudioRole', {
      assumedBy: new iam.ServicePrincipal('elasticmapreduce.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonElasticMapReduceFullAccess'),
      ]
    });
    studioRole.addToPolicy(sharedS3Policy)
    studioRole.addToPolicy(sharedIAMpolicy)

    studioRole.addToPolicy(new iam.PolicyStatement({
      resources: ['arn:aws:emr-serverless:' + process.env.CDK_DEFAULT_REGION + ':' + process.env.CDK_DEFAULT_ACCOUNT + ':/applications/*'],
      actions: ['emr-serverless:AccessInteractiveEndpoints'],
    }));
    studioRole.addToPolicy(new iam.PolicyStatement({
      resources: [appRole.roleArn],
      actions: ['iam:PassRole'],
      conditions: {
        "StringLike": {
          "iam:PassedToService": "emr-serverless.amazonaws.com"
        }
      }
    }));

    // EMR Studio
    const cfnStudio = new emr.CfnStudio(this, 'MyCfnStudio', {
      authMode: 'IAM',
      defaultS3Location: `s3://${bucket.bucketName}/emrstudio/`,
      engineSecurityGroupId: vpc.vpcDefaultSecurityGroup,
      name: 'GenAI-Datalake',
      serviceRole: studioRole.roleArn,
      subnetIds: [vpc.privateSubnets[0].subnetId],
      vpcId: vpc.vpcId,
      workspaceSecurityGroupId: vpc.vpcDefaultSecurityGroup,
    });

    //Manually adding dependencies because studio resource is weird
    cfnStudio.node.addDependency(bucket)
    cfnStudio.node.addDependency(studioRole)
    cfnStudio.node.addDependency(vpc)

    // EMR Studio Application
    const cfnApplication = new emrserverless.CfnApplication(this, 'MyCfnApplication', {
      releaseLabel: 'emr-6.15.0',
      type: 'Spark',
      name: "GenAIDataLakeApp",
      initialCapacity: [{
        key: "Executor",
        value: {
          workerCount: 2,
          workerConfiguration: {
            cpu: "4 vCPU",
            memory: "16 GB",
            disk: "20 GB"
          }
        }
      },
      {
        key: "Driver",
        value: {
          workerCount: 1,
          workerConfiguration: {
            cpu: "4 vCPU",
            memory: "16 GB",
            disk: "20 GB"
          }
        }
      }],
      runtimeConfiguration: [{
        classification: 'spark-defaults	',
        properties: {
          'spark.hadoop.hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory',
        },
      }],

    })

    // SageMaker role
    const smRole = new iam.Role(this, 'SMStudioRole', {
      assumedBy: new iam.ServicePrincipal('sagemaker.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('AmazonSageMakerFullAccess'),
      ]
    });
    smRole.addToPolicy(sharedS3Policy)
    smRole.addToPolicy(sharedIAMpolicy)

    smRole.addToPolicy(new iam.PolicyStatement({
      resources: ['*'],
      actions: ['athena:*'],
    }));
    smRole.addToPolicy(new iam.PolicyStatement({
      resources: ['*'],
      actions: ['bedrock:*'],
    }));
    smRole.addToPolicy(new iam.PolicyStatement({
      resources: ['*'],
      actions: ['bedrock-runtime:*'],
    }));

    // SageMaker Studio
    const cfnDomain = new sagemaker.CfnDomain(this, 'MyCfnDomain', {
      authMode: 'IAM',
      defaultUserSettings: {
        executionRole: smRole.roleArn,
      },
      domainName: 'GenAIDataLakeDomain',
      subnetIds: [vpc.privateSubnets[0].subnetId],
      vpcId: vpc.vpcId,

      // the properties below are optional
      defaultSpaceSettings: {
        executionRole: smRole.roleArn,
      },
    });

    // SageMaker user profile
    const cfnUserProfile = new sagemaker.CfnUserProfile(this, 'MyCfnUserProfile', {
      domainId: cfnDomain.attrDomainId,
      userProfileName: 'GenAIDataLakeUser',
      userSettings: {
        executionRole: smRole.roleArn,
        spaceStorageSettings: {
          defaultEbsStorageSettings: {
            defaultEbsVolumeSizeInGb: 10,
            maximumEbsVolumeSizeInGb: 300,
          },
        },
      },
    });

    //Glue DataQuality
    const glueDataQualityRole = new iam.Role(this, 'GlueDataQualityRole', {
      assumedBy: new iam.ServicePrincipal('glue.amazonaws.com'),
      managedPolicies: [
        iam.ManagedPolicy.fromAwsManagedPolicyName('service-role/AWSGlueServiceRole')
      ]
    });
    glueDataQualityRole.addToPolicy(new iam.PolicyStatement({
      resources: ['*'],
      actions: ['glue:StartJobRun', 'glue:GetJobRun']
    }));
    glueDataQualityRole.addToPolicy(sharedS3Policy)
    glueDataQualityRole.addToPolicy(sharedIAMpolicy)

    // Output the S3 bucket name as a stack output
    new cdk.CfnOutput(this, 'S3BucketName', {
      value: bucket.bucketName,
      description: 'S3 bucket name',
    });
  }
}
