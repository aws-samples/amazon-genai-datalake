#!/usr/bin/env node
import 'source-map-support/register';
import * as cdk from 'aws-cdk-lib';
import { GenAIDataLake } from '../lib/genai-datalake-stack';
import { AwsSolutionsChecks } from 'cdk-nag';

const app = new cdk.App();
new GenAIDataLake(app, 'GenAIDataLake', {
  env: { account: process.env.CDK_DEFAULT_ACCOUNT, region: process.env.CDK_DEFAULT_REGION },
});

//cdk.Aspects.of(app).add(new AwsSolutionsChecks());