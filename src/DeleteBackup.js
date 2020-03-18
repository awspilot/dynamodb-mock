
module.exports = function( client_req, client_res, region, body_json ) {

	console.log("[DynamoDB] DeleteBackup", body_json.BackupArn ) // JSON.stringify(body_json, null, "\t")

	var account_id = '000000000000';

	var BackupArn = body_json.BackupArn.match(/^arn:aws:dynamodb:(?<region>[^:]+):(?<account_id>[0-9]+):table\/(?<table>[^\/]+)\/backup\/(?<timestamp>[^-]+)-(?<backup_id>.*)$/i)

	if (!BackupArn) {
		client_res.statusCode = 404;
		client_res.end()
		return;
	}

	var key = BackupArn.groups.account_id + ' ' + BackupArn.groups.region + ' ' + BackupArn.groups.table + ' ' + BackupArn.groups.backup_id;

	backupdb.get( key , function (err, data ) {


		if (err) {
			client_res.statusCode = 404;
			client_res.end()
			return;
		}

		var key_data;
		try {
			key_data = JSON.parse(data);
		} catch (e) {
		}

		if (!key_data) {
			client_res.statusCode = 404;
			client_res.end()
			return;
		}

		// delete it
		backupdb.del( key , function (err) {
			if (err) {
				client_res.statusCode = 404;
				client_res.end()
				return;
			}

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
		})

	})

}
