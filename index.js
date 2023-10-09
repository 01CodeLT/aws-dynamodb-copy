const { DynamoDBClient, DescribeTableCommand, BatchWriteItemCommand, ScanCommand, CreateTableCommand } = require("@aws-sdk/client-dynamodb");
var readline = require('readline');

class DynamoDBCopy {

    tableName;
    options;

    sourceClient;
    destinationClient;

    constructor({
        tableName,
        source,
        destination
    }) {
        // Set properties
        this.tableName = tableName;

        // Create clients
        this.sourceClient = new DynamoDBClient(source);
        this.destinationClient = new DynamoDBClient(destination);
    }

    async copySchema() {
        // Check source table exists
        const sourceTable = await this._getTable(this.tableName, this.sourceClient);
        if(sourceTable === false || sourceTable.Table.TableStatus !== 'ACTIVE') {
            throw new Error('The source table does not exist!');
        }

        // Create new table
        return await this._createTable(this.destinationClient, sourceTable.Table);
    }

    async copyItems() {
        // Copy items
        const itemsFromSource = await this._getTableItems(this.tableName, this.sourceClient);
        return await this._copyTableItems(this.destinationClient, itemsFromSource);
    }

    async clone(options = {
        createIfNotExist: false
    }) {
        // Check destination table exists
        const destinationTable = await this._getTable(this.tableName, this.destinationClient);
        if(destinationTable === false || destinationTable.Table.TableStatus !== 'ACTIVE') {
            if(options.createIfNotExist === true) {
                // Don't allow copy if failed
                if(await this.copySchema() === false) {
                    return { status: false };
                }
            } else {
                throw new Error("Destination table does not exist, if you'd like to create it automatically set the createIfNotExist option to true.")
            }
        }

        return await this.copyItems();
    }

    async _getTable(tableName, client) {
        try {
            return await client.send(new DescribeTableCommand({
                TableName: tableName
            }));
        } catch(error) {
            // console.log(error);
            return false;
        }
    }

    async _getTableItems(tableName, client) {
        const data = await client.send(new ScanCommand({
            TableName: tableName,
        }));

        return data.Items || [];
    }

    async _createTable(client, schema) {
        this.writeOutput('Creating destination table...');

        // Send create
        await client.send(new CreateTableCommand({
            TableName: this.tableName,
            KeySchema: schema.KeySchema,
            AttributeDefinitions: schema.AttributeDefinitions,
            BillingMode: schema.BillingModeSummary.BillingMode,
            DeletionProtectionEnabled: schema.DeletionProtectionEnabled,
            ...(schema.LocalSecondaryIndexes && { 
                LocalSecondaryIndexes: schema.LocalSecondaryIndexes?.map((index) => {
                    return {
                        IndexName: index.IndexName,
                        KeySchema: index.KeySchema,
                        Projection: index.Projection
                    }
                }) 
            }),
            ...(schema.GlobalSecondaryIndexes && { 
                GlobalSecondaryIndexes: schema.GlobalSecondaryIndexes?.map((index) => {
                    return {
                        IndexName: index.IndexName,
                        KeySchema: index.KeySchema,
                        Projection: index.Projection
                    }
                }) 
            })
        }));

        // Wait for table to be active
        const result = await new Promise((resolve) => {
            // Check status
            const checker = setInterval(() => {
                this.writeOutput('Waiting for table creation...');
                this._getTable(this.tableName, this.destinationClient).then((newTable) => {
                    if(newTable !== false && newTable.Table.TableStatus === 'ACTIVE') {
                        clearInterval(checker);
                        resolve(true);
                    }
                })
            }, 10000);

            // Don't wait more than 1 min
            setTimeout(() => {
                clearInterval(checker);
                resolve(false);
            }, 60000);
        });
        
        this.writeOutput(result === true ? 'Destination table created!' : 'Destination table failed to become active');
        return { status: result };
    }

    async _copyTableItems(client, items) {
        const chunkSize = 24;
        const output = { status: true, UnprocessedItems: [] };
        this.writeOutput('Copying table items');
        
        for (let i = 0; i < items.length; i += chunkSize) {
            this.writeOutput(`Copying chunk ${Math.ceil(i / chunkSize) + 1} of ${Math.ceil(items.length / chunkSize)}`);

            const batchOutput = await client.send(new BatchWriteItemCommand({
                RequestItems: {
                    [this.tableName]: items.slice(i, i + chunkSize).map(function (item, index) {
                        return {
                            PutRequest: {
                                Item: item
                            }
                        }
                    })
                }
            }));

            output.UnprocessedItems = output.UnprocessedItems.concat(
                batchOutput[this.tableName]?.UnprocessedItems || []
            );
        }

        output.status = output.UnprocessedItems.length === 0;
        this.writeOutput(`Copy completed: ${items.length - output.UnprocessedItems.length} of ${items.length} copied successfully`);
        return output;
    }

    async writeOutput(output) {
        readline.clearLine(process.stdout)
        readline.cursorTo(process.stdout, 0)
        process.stdout.write(output);
    }
}

module.exports = { DynamoDBCopy };