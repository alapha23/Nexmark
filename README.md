# Nexmark

Nexmark benchmark explained in details by [Apache Beam documents](https://beam.apache.org/documentation/sdks/java/testing/nexmark/).

### Build

mvn install

### Query 0 locally

java -cp Query0/target/Query0-1.0-client-shaded.jar query0.Query0Launcher

### Internals

Depends on Apache beam 2.6.0.

### How to invoke queries on AWS Lambda

1. cd Query0Lambda26
2. mvn install && serverless deploy
3. Upload library jars [lambda.zip](https://github.com/alapha23/Nexmark/tree/master/Query0Lambda26/package) to AWS Lambda Layer
4. Add AWS Lambda Layer and IAM role to serverless function by WebUI
5. In your terminal, `aws lambda invoke --function-name gao-nextmark-q0 output.txt`, feel free to replace `gao-nextmark-q0` with your name in serverless.yml

Find a list of jars within Lamdba layer zip at [Nexmark/layer_jars](https://github.com/alapha23/Nexmark/blob/master/layer_jars.txt). These jars are what you have uploaded to AWS Lambda Layer.
Feel free to modify serverless.yml to use your own function name and other features. 
Replace `Query0*` by `Query2*` in above instructions to execute query2.

