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
	if (is_demo && (client_req.headers['x-amz-target']) === ('DynamoDB_20120810.DeleteTable') && (demo_tables.indexOf(body_json.TableName) !== -1 ) ) {
		client_res.statusCode = 400;
		client_res.end(JSON.stringify({ err: { errorMessage: 'deleteTable forbidden in demo'}, }));
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




















// ######################## OLD CODE ###############################


console.log("Starting dynamodb proxy server on port 10002")

var AWS = require('aws-sdk');
AWS.config.update({ accessKeyId: "myKeyId", secretAccessKey: "secretKey", region: "us-east-1" })


http.createServer(function (request, response) {
	console.log( "[dynamoproxy]", request.method, request.url )
	if ( request.method === 'POST') {
		var dynamodb = new AWS.DynamoDB({
			endpoint: new AWS.Endpoint('http://localhost:8000'),
			region: request.headers.region || 'us-east-1'
		});

		var body = '';

		request.on('data', function (data) { body += data;});

		request.on('end', function () {

			var event = {
				_POST: JSON.parse(body)
			}
			response.writeHead(200, { 'Content-Type': 'application/json' });

			
			if (is_demo) {

				if ( event._POST.method === 'deleteTable' && (demo_tables.indexOf(event._POST.payload.TableName) !== -1) )
					return response.end(JSON.stringify({ err: { errorMessage: 'deleteTable forbidden in demo'}, }));
				if ( event._POST.method === 'updateTable' && (demo_tables.indexOf(event._POST.payload.TableName) !== -1) )
					return response.end(JSON.stringify({ err: { errorMessage: 'updateTable forbidden in demo'}, }));

				if ( event._POST.method === 'putItem'  && (demo_tables.indexOf(event._POST.payload.TableName) !== -1) )
					return response.end(JSON.stringify({ err: { errorMessage: 'putItem forbidden in demo'}, }));
				if ( event._POST.method === 'deleteItem'  && (demo_tables.indexOf(event._POST.payload.TableName) !== -1) )
					return response.end(JSON.stringify({ err: { errorMessage: 'deleteItem forbidden in demo'}, }));

			}


			switch ( event._POST.method ) {
				case 'listTables':
				case 'createTable':
				case 'deleteTable':
				case 'describeTable':
				case 'describeTimeToLive':
				case 'updateTimeToLive':
				case 'updateTable':
				case 'scan':
				case 'query':
				case 'deleteItem':
				case 'putItem':
					dynamodb[event._POST.method](event._POST.payload, function(err, data) {
						response.end(JSON.stringify({ err: err, data:data }));
					})
					break;
				default:
					response.end(JSON.stringify({err:'not-implemented', event: event }));
			}
		});
		return;
	}
}).listen(10002);
