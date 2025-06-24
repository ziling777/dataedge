# EMR Job Dependencies

This directory should contain the following JAR files for the EMR job to work properly:

## Required JAR Files:
- `aws-java-sdk-bundle-1.12.661.jar` (353.88 MB)
- `awssdk-bundle-2.29.38.jar` (612.39 MB)
- `caffeine-3.1.8.jar`
- `commons-configuration2-2.11.0.jar`
- `hadoop-aws-3.3.4.jar`
- `iceberg-spark-runtime-3.5_2.12-1.6.1.jar`
- `s3-tables-catalog-for-iceberg-0.1.3.jar`

## Download Instructions:
Due to GitHub's file size limitations, these JAR files are not included in the repository. 

You can download them from:
1. Maven Central Repository
2. AWS SDK releases
3. Apache Iceberg releases

## Alternative:
Consider using Maven or Gradle to manage these dependencies automatically in your EMR job configuration.
