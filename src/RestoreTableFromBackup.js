const DynamoFactory = require('@awspilot/dynamodb')

var describeTable_to_sql = function( describeTable, overrideTableName ) {

	var create_table;

	var partitionkeytype = ''
	var partitionkeyname = ''
	describeTable.KeySchema.map(function(k) {
		if ( k.KeyType !== 'HASH')
			return;

		partitionkeyname = k.AttributeName;

		var type;
		describeTable.AttributeDefinitions.map(function( t ) {
			if ( t.AttributeName === k.AttributeName )
				type = t.AttributeType;
		})
		partitionkeytype = k.AttributeName + ' ' + (({S:'STRING',N:'NUMBER',B:'BINARY'})[type]) + ",";
	})

	var sortkeytype = ''
	var sortkeyname = ''

	describeTable.KeySchema.map(function(k) {
		if ( k.KeyType !== 'RANGE')
			return;

		sortkeyname = ' , ' + k.AttributeName;
		var type;
		describeTable.AttributeDefinitions.map(function( t ) {
			if ( t.AttributeName === k.AttributeName )
				type = t.AttributeType;
		})
		sortkeytype = k.AttributeName + ' ' + (({S:'STRING',N:'NUMBER',B:'BINARY'})[type]) + ",";
	})

	if ( (describeTable.BillingModeSummary || {}).BillingMode === 'PAY_PER_REQUEST') {
		create_table = `
			CREATE PAY_PER_REQUEST TABLE \`${overrideTableName}\` (
				${partitionkeytype}
				${sortkeytype}
				PRIMARY KEY ( ${partitionkeyname} ${sortkeyname} )
			)
		`;
	} else {
		create_table = `
			CREATE PROVISIONED TABLE \`${overrideTableName}\` (
				${partitionkeytype}
				${sortkeytype}
				PRIMARY KEY ( ${partitionkeyname} ${sortkeyname} ) THROUGHPUT ${describeTable.ProvisionedThroughput.ReadCapacityUnits} ${describeTable.ProvisionedThroughput.WriteCapacityUnits}
			)
		`;
	}

	return create_table;
}


module.exports = function( client_req, client_res, region, body_json, auth ) {

	// we connect to local dynamodb,
	var DynamoDB = new DynamoFactory({
		accessKeyId: auth.accessKeyId,
		secretAccessKey: 'x',
		region: region,
		endpoint: 'http://localhost:8000',
	})

	// unlike aws, backups are created into user's s3
	var s3 = new AWS.S3({
		accessKeyId: auth.accessKeyId,
		secretAccessKey: 'x',
		region: region,
		endpoint: process.env.DYNAMODBMOCK_BACKUP_S3_ENDPOINT,
		s3ForcePathStyle: true,
	});


	console.log("[DynamoDB] RestoreTableFromBackups", region ) // JSON.stringify(body_json, null, "\t")


	var account_id = '000000000000';

	var BackupArn = body_json.BackupArn.match(/^arn:aws:dynamodb:(?<region>[^:]+):(?<account_id>[0-9]+):table\/(?<table>[^\/]+)\/backup\/(?<timestamp>[^-]+)-(?<backup_id>.*)$/i)

	if (!BackupArn) {
		client_res.statusCode = 404;
		client_res.end()
		return;
	}
	//console.log( body_json )

	var key = BackupArn.groups.account_id + ' ' + BackupArn.groups.region + ' ' + BackupArn.groups.table + ' ' + BackupArn.groups.backup_id;

	var key_data;
	var newDescribeTable;
	async.waterfall([

		// step1, get backup from db
		function(cb) {
			backupdb.get( key , function (err, data ) {

				if (err)
					return cb(false) // end with error

				try {
					key_data = JSON.parse(data);
				} catch (e) {
					return cb(false) // end with error
				}

				if (!key_data)
					return cb(false) // end with error

				cb()
			})
		},

		// step2, make sure target table does not exist
		function( cb ) {
			DynamoDB.client.describeTable({TableName: body_json.TargetTableName}, function( err, data ) {

				if (err && err.code === 'ResourceNotFoundException')
					return cb() // perfect

				if (err) {
					console.log( "[DynamoDB] RestoreTableFromBackups describeTable", err )
					return cb( false );
				} else {
					// exists
					console.log( "[DynamoDB] RestoreTableFromBackups target table already exists" )
					return cb( false );
				}

				cb()
			})
		},

		// step3, mark as restoring
		function( cb ) {
			key_data.BackupStatus = 'RESTORING';

			backupdb.put( key , JSON.stringify(key_data), function (err) {
				if (err)
					return cb(false)

				cb()
			})
		},

		// step4, create table ( no indexes atm )
		function( cb ) {
			var create_table = describeTable_to_sql( key_data.describeTable, body_json.TargetTableName )

			DynamoDB.query(create_table, function(err, data ) {
				if (err) {
					console.log( "[DynamoDB] RestoreTableFromBackups createTable", err )
					return cb(false)
				}

				newDescribeTable = data;

				// @todo add this to newDescribeTable
				newDescribeTable.RestoreSummary = {
					RestoreDateTime: new Date().getTime()/1000,
					RestoreInProgress: true,
					SourceBackupArn: `arn:aws:dynamodb:${region}:${account_id}:table/${key_data.TableName}/backup/${key_data.created_at}-${key_data.backup_id}`,
					SourceTableArn: `arn:aws:dynamodb:${region}:${account_id}:table/${key_data.TableName}`,
				}

				cb()
			})


		},

		// step5, end client connection with success, continue in background
		function( cb ) {
			client_res.writeHead( 200, {
				'Access-Control-Allow-Origin': '*',
				'Access-Control-Expose-Headers': 'x-amzn-RequestId,x-amzn-ErrorType,x-amzn-ErrorMessage,Date',
				Connection: 'keep-alive',
				//Content-Length: 256
				'Content-Type': 'application/x-amz-json-1.0',
				//Date: Wed, 18 Mar 2020 11:52:59 GMT
				Server: 'Server',
				//x-amz-crc32: 782095031
				'x-amzn-RequestId': 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
			});



			client_res.end(JSON.stringify({
				TableDescription: newDescribeTable
			}, null, "\t"))
			cb()
		},


		// step6, wait for table to be ACTIVE
		function( cb ) {
			var interval = setInterval( function() {
				DynamoDB.client.describeTable({TableName: body_json.TargetTableName}, function( err, data ) {
					if (err) {
						clearInterval(interval)
						return cb(err)
					}

					if (data.Table.TableStatus === 'ACTIVE') {
						clearInterval(interval)
						return cb()
					}

					// do nothing let the interval check TableStatus again
				})
			}, 3000)
		},




		// step7, read data and fill in dynamodb
		function( cb ) {
			s3.getObject({
				Bucket: 'dynamodb-backups',
				Key: key_data.s3_key,
			}, function( err, data ) {
				if (err)
					return cb(err);

				var sqls = data.Body.toString().split("\n");
				console.log("s3.getObject", err, sqls )
				async.eachSeries( sqls, function( sql, cb ) {
					if (!sql.trim())
						return cb()

					console.log(" -| ", sql )

					var regex = /INSERT\sINTO\s\`?(?<table>[^(`|\s)]+)\`?\sVALUES\s?(?<dump>.*)/i;
					var insert_sql = sql.match( regex );
					if (!insert_sql)
						return cb(); // skip

					sql = "INSERT INTO `" + body_json.TargetTableName + "` VALUES " + insert_sql.groups.dump;

					DynamoDB.query(sql, function(err) {
						if (err)
							console.log(err)
						cb()
					})
				}, function(err) {
					cb()
				})

			})
		},


		// step8, mark as available again
		function( cb ) {
			key_data.BackupStatus = 'AVAILABLE';

			backupdb.put( key , JSON.stringify(key_data), function (err) {
				if (err)
					return cb(false)

				cb()
			})
		},

	], function(err) {
		if ( err === false ) {
			reply_with_error( client_res )
		}

	})







}
