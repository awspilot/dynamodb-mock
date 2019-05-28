#!/usr/bin/env node
console.log("Starting dynamodb proxy server on port 10004")

var AWS = require('aws-sdk')
var http = require('http')
var is_demo = process.env.DEMO == '1';
var demo_tables = [ 'cities','countries' ];
console.log("demo is ", is_demo ? 'ON' : 'OFF' )

http.createServer(function (client_req, client_res) {

	var body = '';
	client_req.on('data', function (data) {body += data;});
	client_req.on('end', function () {

		// if OPTIONS , reply with CORS '*'
		if (client_req.method === 'OPTIONS') {
			client_res.writeHead(204, {
				'Access-Control-Allow-Origin': client_req.headers['origin'] || '*',
				'Access-Control-Allow-Methods': 'GET, POST, PUT, DELETE, OPTIONS, HEAD',
				'Access-Control-Allow-Headers': 'content-type, authorization, x-amz-content-sha256, x-amz-date, x-amz-target, x-amz-user-agent',
				'Access-Control-Max-Age': 2592000, // 30 days

				// 'Content-Type': 'text/html;charset=UTF-8',
				// 'Content-Length': '0',
			});
			client_res.end();
			return;
		}


		var auth_re = /(?<algorithm>[A-Z0-9\-]+)\ Credential=(?<accesskey>[^\/]+)\/(?<unknown1>[^\/]+)\/(?<region>[^\/]+)\/([^\/]+)\/([^,]+), SignedHeaders=(?<signed_headers>[^,]+), Signature=(?<signature>[a-z0-9]+)/

		var auth = (client_req.headers['authorization'] || '') .match( auth_re );
		if (  auth === null )
			return client_res.end('Failed auth');

		console.log("auth region=",auth.groups.region )

		var body_json = null
		try {
			body_json = JSON.parse(body)
		} catch (err) {
			console.log(err)
		}

		console.log("body json=", JSON.stringify(body_json, null, "\t") )

		if (is_demo && (client_req.headers['x-amz-target'] === 'DynamoDB_20120810.DeleteTable') && (demo_tables.indexOf(body_json.TableName) !== -1 ) ) {
			client_res.statusCode = 400;
			client_res.end(JSON.stringify({"__type":"com.amazonaws.dynamodb.v20120810#ResourceForbiddenException","message":"Cannot delete demo tables"}));
			return;
		}
		if (is_demo && (client_req.headers['x-amz-target'] === 'DynamoDB_20120810.UpdateTable') && (demo_tables.indexOf(body_json.TableName) !== -1 ) ) {
			client_res.statusCode = 400;
			client_res.end(JSON.stringify({"__type":"com.amazonaws.dynamodb.v20120810#ResourceForbiddenException","message":"Cannot update demo tables"}));
			return;
		}
		if (is_demo && (client_req.headers['x-amz-target'] === 'DynamoDB_20120810.DeleteItem') && (demo_tables.indexOf(body_json.TableName) !== -1 ) ) {
			client_res.statusCode = 400;
			client_res.end(JSON.stringify({"__type":"com.amazonaws.dynamodb.v20120810#ResourceForbiddenException","message":"Cannot items from demo tables"}));
			return;
		}





		if (client_req.headers['x-amz-target'] === 'DynamoDB_20120810.ListBackups') {
			client_res.end(JSON.stringify({
				BackupSummaries: []
			}));
			return;
		}

		var cloudwatch = new AWS.CloudWatch({
			endpoint: process.env.CW_ENDPOINT,
			region: auth.groups.region,
			credentials: {
				accessKeyId: 'x',
				secretAccessKey: 'y',
			}
		});



		console.log("received request ",JSON.stringify({
			url: client_req.url,
			hostname: client_req.hostname,
			host: client_req.host,
			port: client_req.port,
			path: client_req.path,
			method: client_req.method,
			headers: client_req.headers,
			//timeout // ms
		}, null, "\t"));

		client_req.headers.host = 'localhost';
		var proxy_options = {
			host: 'localhost',
			port: 8000,
			path: '/',
			method: client_req.method,
			headers: client_req.headers,
		}


		//console.log("proxying request to ", JSON.stringify(proxy_options, null,"\t"))

		var req=http.request(proxy_options, function(res) {
			var body = '';
			res.on('data', function (chunk) {
				body += chunk;
			});
			res.on('end', function () {

				if ( res.statusCode === 400 ) {
						var params = {
							MetricData:[{
								MetricName: 'UserErrors',
								Timestamp:  new Date,
								Value: 1,
							}],
							Namespace: 'AWS/DynamoDB',
						};
						cloudwatch.putMetricData( params, console.log );
				}
				if ( res.statusCode === 500 ) {
						var params = {
							MetricData:[{
								MetricName: 'SystemErrors',
								Timestamp:  new Date,
								Value: 1,
							}],
							Namespace: 'AWS/DynamoDB',
						};
						cloudwatch.putMetricData( params, console.log );
				}

				// ProvisionedReadCapacityUnits, ProvisionedWriteCapacityUnits
				// WriteThrottleEvents , ReadThrottleEvents,

				if ( res.statusCode === 200 ) {

					var response_body_json;
					try {
						response_body_json = JSON.parse(body)
					} catch (e) {}


					if (
						(client_req.headers['x-amz-target'] === "DynamoDB_20120810.PutItem") ||
						(client_req.headers['x-amz-target'] === "DynamoDB_20120810.UpdateItem")
					) {

						// step1, calculare payload size
						var payload_size = 1;
						if (body_json.hasOwnProperty('Item')) // for PutItem
							payload_size = JSON.stringify(body_json.Item).length;

						if (body_json.hasOwnProperty('AttributeUpdates')) // for UpdateItem
							payload_size = JSON.stringify(body_json.AttributeUpdates).length; // this is not fair as the item size should include existing attributes as well

						// A map of attribute values as they appear before or after the UpdateItem operation, more accurate than payload.AttributeUpdates
						if (response_body_json.hasOwnProperty('Attributes') && (client_req.headers['x-amz-target'] === "DynamoDB_20120810.UpdateItem"))
							payload_size = JSON.stringify(response_body_json.Item || {}).length;

						// step2, calculare write capacity units based on payload size
						var write_capacity_units = Math.ceil(payload_size / 4096);

						if (((response_body_json || {}).ConsumedCapacity || {}).WriteCapacityUnits > 0 )
							write_capacity_units = parseFloat(response_body_json.ConsumedCapacity.WriteCapacityUnits)

						var params = {
							MetricData:[{
								MetricName: 'ConsumedWriteCapacityUnits',
								Timestamp:  new Date,
								Value: write_capacity_units,
								Dimensions: [
									{
										Name: 'TableName',
										Value: body_json.TableName
									},
								],
							}],
							Namespace: 'AWS/DynamoDB',
						};
						cloudwatch.putMetricData(params, console.log );
					}

					if (
							(client_req.headers['x-amz-target'] === "DynamoDB_20120810.Scan") ||
							(client_req.headers['x-amz-target'] === "DynamoDB_20120810.Query") ||
							(client_req.headers['x-amz-target'] === "DynamoDB_20120810.GetItem")
					) {

						// step1, calculare response size
						var response_size = 1;
						if (response_body_json.hasOwnProperty('Item')) // for GetItem
							response_size = JSON.stringify(response_body_json.Item).length;

						if (response_body_json.hasOwnProperty('Items')) // for Query and Scan
							response_size = JSON.stringify(response_body_json.Items).length;

						// step2, calculare read capacity units based on response size
						var read_capacity_units = Math.ceil(response_size / 4096) / 2;
						if ( body_json.ConsistentRead === true )
							read_capacity_units = Math.ceil(response_size / 4096); // each 4K costs 1 read

						if (((response_body_json || {}).ConsumedCapacity || {}).ReadCapacityUnits > 0 )
							read_capacity_units = parseFloat(response_body_json.ConsumedCapacity.ReadCapacityUnits)


						var params = {
							MetricData:[{
								MetricName: 'ConsumedReadCapacityUnits',
								Timestamp:  new Date,
								Value: read_capacity_units,
								Dimensions: [
									{
										Name: 'TableName',
										Value: body_json.TableName
									},
								],
							}],
							Namespace: 'AWS/DynamoDB',
						};
						cloudwatch.putMetricData(params, console.log );
					}


				}

				console.log("proxy ended")
				res.headers['Access-Control-Allow-Origin'] = '*'
				client_res.writeHead(res.statusCode, res.headers);
				client_res.end(body);
			});
		});
		req.on('error', function(err) {
			console.log("proxy errored")
			console.log("target error")
			client_res.end('error: ' + err.message);
		});
		req.write(body);
		req.end();



	});
}).listen(10004);
