
module.exports = function( client_req, client_res, region, body_json ) {

	console.log("[DynamoDB] ListBackups", region ) // JSON.stringify(body_json, null, "\t")


	var account_id = '000000000000';

	var backups_to_return = []

	backupdb.createReadStream({
		gt: account_id + ' ' + region + ' ' + body_json.TableName + ' ',
		lt: account_id + ' ' + region + ' ' + body_json.TableName + 'Z',
		limit: 50,
	})
	.on('data', function (data) {
		var backup_data;
		try {
			backup_data = JSON.parse(data.value);
			backups_to_return.push({
				BackupArn: `arn:aws:dynamodb:${backup_data.region}:${backup_data.account_id}:table/${backup_data.TableName}/backup/${backup_data.created_at}-${backup_data.backup_id}`,
				BackupCreationDateTime: backup_data.created_at/1000,
				BackupName: backup_data.BackupName,
				BackupSizeBytes: backup_data.BackupSizeBytes,
				BackupStatus: backup_data.BackupStatus,
				BackupType: backup_data.BackupType,
				TableArn: `arn:aws:dynamodb:${backup_data.region}:${backup_data.account_id}:table/${backup_data.TableName}`,
				TableId:"xxxxxxxx-xxxx-xxxx-xxxx-xxxxxxxxxxxx",
				TableName: backup_data.TableName,

				// custom, not available in AWS, unfortunately aws-sdk wont passtrough
				items: backup_data.items,
			})
		} catch (e) {
		}
	})
	.on('error', function (err) {

	})
	.on('close', function () {

	})
	.on('end', function () {

		client_res.writeHead( 200, {
			'Access-Control-Allow-Origin': '*',
			'Access-Control-Expose-Headers': 'x-amzn-RequestId,x-amzn-ErrorType,x-amzn-ErrorMessage,Date',
			Connection: 'keep-alive',
			//Content-Length: 785
			'Content-Type': 'application/x-amz-json-1.0',
			//Date: Wed, 18 Mar 2020 11:50:21 GMT
			Server: 'Server',
			//x-amz-crc32: 3484744928
			'x-amzn-RequestId': 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX',
		});

		client_res.end(JSON.stringify({
			BackupSummaries: backups_to_return.sort(function(a, b ) {
				return a.BackupCreationDateTime > b.BackupCreationDateTime ? -1 : 1;
			})
		}));


	})


}
