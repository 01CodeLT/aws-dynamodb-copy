# AWS DynamoDB Copy
An AWS utility for copying dynamodb tables accross AWS accounts. This package has been developed using the AWS SDK v3 from logic based on Mahmoud Marie's 'copy-dynamodb-table' package (https://github.com/enGMzizo/copy-dynamodb-table).

### Usage

Import the plugin
```
const { DynamoDBCopy } = require('@01coder/dynamodb-copy');
const dynamodbCopyInstance = new DynamoDBCopy({
    tableName: source.tableName, // Table name to copy from
    source: {
        region: "eu-west-2", 
        credentials: fromSSO({}) // Provide a method from @aws-sdk/credential-providers
    },
    destination: {
        region: "eu-west-2", 
        credentials: fromSSO({})
    }
})
```

Methods
```
dynamodbCopyInstance.copySchema();

dynamodbCopyInstance.copyItems();

// Copy items and schema
dynamodbCopyInstance.copyItems({
    createIfNotExist: true // Create table if it doesn't exist
}).then((result) => {
    if(result.status == true) {

    }
})
```

*Please note: This package is yet to be fully reviewed and tested...*
