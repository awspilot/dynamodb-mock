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
				'Access-Control-Allow-Origin': '*',
				'Access-Control-Allow-Methods': 'OPTIONS, POST, GET',
				'Access-Control-Max-Age': 2592000, // 30 days
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
					if ( (client_req.headers['x-amz-target'] === "DynamoDB_20120810.PutItem") || (client_req.headers['x-amz-target'] === "DynamoDB_20120810.UpdateItem") ) {

						// @todo: take value from ConsumedCapacity.CapacityUnits
						// @todo: if no comsumed capacity in response, use payload size + read consistency 

						var params = {
							MetricData:[{
								MetricName: 'ConsumedWriteCapacityUnits',
								Timestamp:  new Date,
								Value: 1,
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
						// @todo: take value from ConsumedCapacity.CapacityUnits
						// @todo: if no comsumed capacity in response, use payload size + read consistency 
						var params = {
							MetricData:[{
								MetricName: 'ConsumedReadCapacityUnits',
								Timestamp:  new Date,
								Value: 1,
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
