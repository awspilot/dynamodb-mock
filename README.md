# dynamodb-mock

Not a dynamodb implementation!  
It acts as a middleware between client and DynamoDB local  

It's purpose is to

 - [x] act as a DynamoDB endpoint
 - [x] proxy requests to DynamoDB local
 - [x] intercept and handle backup requests ( create, delete, restore, list )
 - [x] publish cloudwatch metrics ( partially working )
 - [ ] map lambda function to DynamoDB local streams
 - [x] if demo mode, prevent deleting demo tables and items

This is a subproject of @awspilot/dynamodb-ui
