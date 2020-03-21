
module.exports = function( client_req, client_res, region, body_json, auth ) {

	// unline aws, backups are created into user's s3
	var s3 = new AWS.S3({
		accessKeyId: auth.accessKeyId,
		secretAccessKey: 'x',
		region: region,
		endpoint: process.env.DYNAMODBMOCK_BACKUP_S3_ENDPOINT,
		s3ForcePathStyle: true,
	});

	console.log("[DynamoDB] DeleteBackup", body_json.BackupArn ) // JSON.stringify(body_json, null, "\t")

	var account_id = '000000000000';

	var BackupArn = body_json.BackupArn.match(/^arn:aws:dynamodb:(?<region>[^:]+):(?<account_id>[0-9]+):table\/(?<table>[^\/]+)\/backup\/(?<timestamp>[^-]+)-(?<backup_id>.*)$/i)

	if (!BackupArn) {
		client_res.statusCode = 404;
		client_res.end()
		return;
	}

	var key = BackupArn.groups.account_id + ' ' + BackupArn.groups.region + ' ' + BackupArn.groups.table + ' ' + BackupArn.groups.backup_id;

	var key_data;
	async.waterfall([


		// step1, get the backup
		function( cb ) {
			backupdb.get( key , function (err, data ) {

				if (err)
					return reply_with_error( client_res )

				try {
					key_data = JSON.parse(data);
				} catch (e) {
					return reply_with_error( client_res )
				}

				if (!key_data)
					return reply_with_error( client_res )

				cb()
			})
		},


		// step2, mark status as deleting
		function( cb ) {
			key_data.BackupStatus = 'DELETING';

			backupdb.put( key , JSON.stringify(key_data), function (err) {
				if (err)
					return cb(err)

				cb()
			})
		},

		// step3, end client connection, continue in background
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
			client_res.end()
			cb()
		},


		// step3, delete the s3 backup file
		function( cb ) {
			s3.deleteObject({
				Bucket: 'dynamodb-backups',
				Key: key_data.s3_key,
			}, function( err, data ) {
				setTimeout( cb , 3000)
			})
		},


		// step4, delete the backup record
		function( cb ) {
			backupdb.del( key , function (err) {
				cb()
			})
		}

	], function() {
	})



}
