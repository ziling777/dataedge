import boto3

glue_client = boto3.client('glue')



catalog_input = {
    "Name": "s3tablescatalog",
    "CatalogInput": {
        "FederatedCatalog": {
            "Identifier": f"arn:aws:s3tables:us-east-1:123456789012:bucket/*",
            "ConnectionName": "aws:s3tables"
        },
        "CreateDatabaseDefaultPermissions": [],
        "CreateTableDefaultPermissions": []
    }
}

glue_client.create_catalog(**catalog_input)

