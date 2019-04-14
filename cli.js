#!/usr/bin/env node
console.log("Starting dynamodb proxy server on port 10004")

var http = require('http')
var is_demo = process.env.DEMO == '1';
var demo_tables = [ 'cities','countries' ];
console.log("demo is ", is_demo ? 'ON' : 'OFF' )

http.createServer(function (client_req, client_res) {

var body = '';
client_req.on('data', function (data) {body += data;});
client_req.on('end', function () {

	var body_json = null
	try {
		body_json = JSON.parse(body)
	} catch (err) {
		console.log(err)
	}

	console.log("body=", typeof body, body )
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


	console.log("proxying request to ", JSON.stringify(proxy_options, null,"\t"))

	var req=http.request(proxy_options, function(res) {
		var body = '';
		res.on('data', function (chunk) {
			body += chunk;
		});
		res.on('end', function () {
			console.log("proxy ended")
			client_res.writeHead(res.statusCode, res.headers);
			client_res.end(body);
		});
	});
	req.on('error', function(err) {
		console.log("proxy errored")
		console.log("target error")
		client_req.end('error: ' + err.message);
	});
	req.write(body);
	req.end();



});
}).listen(10004);




