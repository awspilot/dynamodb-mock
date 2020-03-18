module.exports = function( client_req, client_res, region, body_json ) {


	//console.log("CreateBackup=", JSON.stringify(body_json, null, "\t") )

	var account_id = '000000000000';
	var backup_id = require('crypto').createHash('md5').update( Math.random().toString() ).digest("hex").slice(0,8)
	var key = account_id + ' ' + region + ' ' + body_json.TableName + ' ' + backup_id;
	var value = {
		account_id: account_id,
		region: region,
		TableName: body_json.TableName,
		BackupName: body_json.BackupName,
		created_at: new Date().getTime(),
		backup_id: backup_id,
		BackupSizeBytes: 0,
		BackupStatus: "CREATING", // "AVAILABLE",
		BackupType:"USER",
	}

	backupdb.put( key , JSON.stringify(value), function (err) {
		if (err) {
			client_res.statusCode = 404;
			return client_res.end()
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

	})




}
