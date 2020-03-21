
const DynamoFactory = require('@awspilot/dynamodb')



module.exports = function( client_req, client_res, region, body_json, auth ) {


	// we connect to local dynamodb,
	var DynamoDB = new DynamoFactory({
		accessKeyId: auth.accessKeyId,
		secretAccessKey: 'x',
		region: region,
		endpoint: 'http://localhost:8000',
	})

	// unline aws, backups are created into user's s3
	var s3 = new AWS.S3({
		accessKeyId: auth.accessKeyId,
		secretAccessKey: 'x',
		region: region,
		endpoint: process.env.DYNAMODBMOCK_BACKUP_S3_ENDPOINT,
		s3ForcePathStyle: true,
	});

	console.log("[DynamoDB] CreateBackup", region, body_json.TableName ) // JSON.stringify(body_json, null, "\t")

	var account_id = '000000000000';
	var backup_id = require('crypto').createHash('md5').update( Math.random().toString() ).digest("hex").slice(0,8)
	var key = account_id + ' ' + region + ' ' + body_json.TableName + ' ' + backup_id;
	var value = {
		account_id: account_id,
		region: region,
		created_at: new Date().getTime(),
		backup_id: backup_id,
		s3_key: body_json.TableName + '-' + region + '-' + (new Date().toISOString().slice(0,16).split(':').join('-')) + '-' + backup_id + '.sql',
		items: 0,

		TableName: body_json.TableName,
		BackupName: body_json.BackupName,
		BackupSizeBytes: 0,
		BackupStatus: "CREATING", //  CREATING | AVAILABLE | DELETED | FAILED(awspilot) | DELETING(awspilot)
		BackupType: "USER", // USER | SYSTEM | AWS_BACKUP
	}


	var scan_stream;
	var scan_buf;
	var describeTable;

	async.waterfall([

		// step1, describe the table
		function( cb ) {
			DynamoDB.client.describeTable({TableName: body_json.TableName}, function( err, data ) {
				if (err) {
					console.log( "[DynamoDB] CreateBackup describeTable", err )
					reply_with_error( client_res )
					return cb( false );
				}

				if (data.Table.TableStatus !== 'ACTIVE') {
					reply_with_error( client_res )
					return cb( false );
				}


				value.describeTable = data.Table;
				console.log("[DynamoDB] CreateBackup describeTable")

				cb()
			})
		},


		// step2, create backup record in db
		function( cb ) {
			backupdb.put( key , JSON.stringify(value), function (err) {
				if (err)
					return cb(err)

				cb()
			})
		},

		// step3, end response to the client, continue in background
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
				BackupDetails:{
					BackupArn: `arn:aws:dynamodb:${region}:${account_id}:table/${body_json.TableName}/backup/${value.created_at}-${value.backup_id}`,
					// "BackupCreationDateTime":1.584532379393E9,
					BackupName: value.BackupName,
					// "BackupSizeBytes":84973,
					BackupStatus: value.BackupStatus,
					BackupType: value.BackupType,
				}
			}));
			cb() // continue
		},


		// step4, create bucket, let it fail if already exists
		function( cb ) {
			s3.createBucket({Bucket: 'dynamodb-backups',}, function( err, data ) {
				if (err && err.code === 'BucketAlreadyExists')
					return cb()

				if (err) {
					console.log( "[DynamoDB] CreateBackup s3.createBucket", err )
					return cb()
				}

				console.log("[DynamoDB] CreateBackup s3.createBucket OK" )
				cb()
			})
		},

		// step5, san the table
		function( cb ) {
			DynamoDB
				.query('SCAN * FROM ' + body_json.TableName + ' INTO STREAM', function( err, data ) {
					if (err)
						return cb(err)

					scan_stream = data;

					var bufs = [];
					scan_stream.on('data', function(d){ bufs.push(d); });
					scan_stream.on('end', function(){
						scan_buf = Buffer.concat(bufs);
						cb()
					})
				})
		},


		// step4, write data to s3
		function( cb ) {

			//data.pipe(process.stdout, { end: false })

			s3.putObject({
				Bucket: 'dynamodb-backups',
				Key: value.s3_key,
				Body: scan_buf,
			}, function( err ) {
				console.log("[DynamoDB] CreateBackup s3.putObject", err )
				if (err)
					return cb(err)

				cb()
			})
		},

	], function( err ) {
		if (err) {

			if (err !== false ) { // false means no becup record has been created
				value.BackupStatus = 'FAILED';
				backupdb.put( key , JSON.stringify(value), function (err) { })
			}
			return;
		}

		value.BackupStatus = 'AVAILABLE';
		value.BackupSizeBytes = scan_buf.length;
		scan_buf = scan_buf.toString().split("\n").length;
		value.items = scan_buf;

		if (value.items > 0) // theres an extra new line after the last query
			value.items--;

		backupdb.put( key , JSON.stringify(value), function (err) { })
	})

}
