import * as cdk from 'aws-cdk-lib';
import { Construct } from 'constructs';
import * as ec2 from 'aws-cdk-lib/aws-ec2';
import * as s3 from 'aws-cdk-lib/aws-s3';
import * as iam from 'aws-cdk-lib/aws-iam';
import { aws_emr as emr } from 'aws-cdk-lib';
import { aws_emrserverless as emrserverless } from 'aws-cdk-lib';
import { aws_sagemaker as sagemaker } from 'aws-cdk-lib';
import { Activity, Persona } from '@cdklabs/cdk-aws-sagemaker-role-manager';

export class GenAIDataLake extends cdk.Stack {
  constructor(scope: Construct, id: string, props?: cdk.StackProps) {
    super(scope, id, props);

    // VPC for EMR and SageMaker Studios
    const vpc = new ec2.Vpc(this, 'Vpc', {
      maxAzs: 2,
    });

    // S3 bucket for Studio storage
    const bucket = new s3.Bucket(this, 's3Bucket', {
      encryption: s3.BucketEncryption.KMS,
      bucketKeyEnabled: true,
      enforceSSL: true,
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

    //check if bucket key is null and then provide "nokey" as the default value
    const bucketKey = bucket.encryptionKey ? bucket.encryptionKey.keyArn : "nokey";

    const sharedIAMpolicy = new iam.PolicyStatement({
      resources: [
        bucketKey
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
      actions: [
        "glue:CreateTable",
        "glue:CreateDatabase",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:DeleteDatabase",
        "glue:UpdateDatabase"
      ]
    }));
    appRole.addToPolicy(sharedS3Policy)
    appRole.addToPolicy(sharedIAMpolicy)

    // EMR Studio role
    const studioRole = new iam.Role(this, 'EMRStudioRole', {
      assumedBy: new iam.ServicePrincipal('elasticmapreduce.amazonaws.com'),
    });
    studioRole.addToPolicy(new iam.PolicyStatement({
      resources: ['*'],
      actions: [
        "ec2:CreateNetworkInterface",
        "ec2:CreateNetworkInterfacePermission",
        "ec2:DeleteNetworkInterface",
        "ec2:DeleteNetworkInterfacePermission",
        "ec2:DescribeNetworkInterfaces",
        "ec2:ModifyNetworkInterfaceAttribute",
        "ec2:AuthorizeSecurityGroupEgress",
        "ec2:AuthorizeSecurityGroupIngress",
        "ec2:CreateSecurityGroup",
        "ec2:DescribeSecurityGroups",
        "ec2:RevokeSecurityGroupEgress",
        "ec2:DescribeTags",
        "ec2:DescribeInstances",
        "ec2:DescribeSubnets",
        "ec2:DescribeVpcs",
        "elasticmapreduce:ListInstances",
        "elasticmapreduce:DescribeCluster",
        "elasticmapreduce:ListSteps",
        "secretsmanager:GetSecretValue",
        "ec2:CreateTags",
        "glue:CreateTable",
        "glue:CreateDatabase",
        "glue:UpdateTable",
        "glue:DeleteTable",
        "glue:GetDatabase",
        "glue:GetDatabases",
        "glue:GetTable",
        "glue:DeleteDatabase",
        "glue:UpdateDatabase",
        "iam:GetUser",
        "iam:GetRole",
        "iam:ListUsers",
        "iam:ListRoles",
        "sso:GetManagedApplicationInstance",
        "sso-directory:SearchUsers"],
    }));
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
        classification: 'spark-defaults',
        properties: {
          'spark.hadoop.hive.metastore.client.factory.class': 'com.amazonaws.glue.catalog.metastore.AWSGlueDataCatalogHiveClientFactory',
        },
      }],

    })

    // SageMaker role
    const smRole = new iam.Role(this, 'SMStudioRole', {
      assumedBy: new iam.ServicePrincipal('sagemaker.amazonaws.com'),
    });

    const smPolicyDoc = iam.PolicyDocument.fromJson(
      require(`${__dirname}/sagemaker-iam.json`)
    )

    const smPolicy = new iam.Policy(this, 'SMStudioPolicy', {
      document: smPolicyDoc,
    })

    smPolicy.attachToRole(smRole)


    smRole.addToPrincipalPolicy(sharedS3Policy)
    smRole.addToPrincipalPolicy(sharedIAMpolicy)

    smRole.addToPrincipalPolicy(new iam.PolicyStatement({
      resources: ['*'],
      actions: [
        "sagemaker:CreateSpace",
        "sagemaker:AddTags",
        "sagemaker:CreatePresignedDomainUrl",
        "athena:GetQueryExecution",
        "athena:GetQueryResults",
        "athena:StartQueryExecution",
        "athena:GetTableMetadata",
        "athena:ListTableMetadata"],
    }));
    smRole.addToPrincipalPolicy(new iam.PolicyStatement({
      resources: ['*'],
      actions: [
        "bedrock:InvokeAgent",
        "bedrock:InvokeModel",
        "bedrock:InvokeModelWithResponseStream"],
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
