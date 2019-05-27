# Nexmark

### Build

mvn install

### Query 0

java -cp Query0/target/Query0-1.0-client-shaded.jar query0.Query0Launcher

### Internals

Depends on Apache beam 2.11

### How to invoke

1. upload jar to S3
2. create lambda function with S3 attached
3. `aws lambda invoke --function-name gao-nextmark-q0 output.txt`, feel free to replace `gao-nextmark-q0` with your name in 2
