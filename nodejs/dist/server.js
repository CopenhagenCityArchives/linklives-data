"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
var express = require("express");
var app = express();
var proxy = require('express-http-proxy');
var rateLimit = require("express-rate-limit");
var resultSizeLimiter = require('./ResultSizeLimiter');
var ipChecker = require("./IpSourceLimiter");
// Enable if you're behind a reverse proxy (Heroku, Bluemix, AWS ELB, Nginx, etc)
// see https://expressjs.com/en/guide/behind-proxies.html
app.set('trust proxy', 1);
var limitRequestsByIp = rateLimit({
    windowMs: 15 * 60 * 1000,
    max: 5 // limit each IP to 100 requests per windowMs
});
var limitResultSize = resultSizeLimiter.ResultSizeLimiter;
var checkIpSource = ipChecker;
// apply the following to all requests:
// proxy requests to elastic search service
// * If post, check ip
// * Limit requests by ip
// * Set limit on result size when searching
//hej
app.use('*', [
    checkIpSource,
    limitRequestsByIp,
    limitResultSize,
    proxy('ll-es:9200')
]);
app.get('/', function (req, res) {
    res.send('hello world!');
});
app.listen(80, function () {
    console.log("listening on port 80.");
});
