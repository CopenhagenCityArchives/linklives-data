import express = require('express');

const app : express.Application = express();
const proxy = require('express-http-proxy');
const rateLimit = require("express-rate-limit");
const resultSizeLimiter = require('./ResultSizeLimiter');
const ipChecker = require("./IpSourceLimiter");

// Enable if you're behind a reverse proxy (Heroku, Bluemix, AWS ELB, Nginx, etc)
// see https://expressjs.com/en/guide/behind-proxies.html
app.set('trust proxy', 1);

const limitRequestsByIp = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 5 // limit each IP to 100 requests per windowMs
  }); 

const limitResultSize = resultSizeLimiter;
  
const checkIpSource = ipChecker;
  
// apply the following to all requests:
// proxy requests to elastic search service
// * If post, check ip
// * Limit requests by ip
// * Set limit on result size when searching

app.use(express.json());
app.use([ 
 //   checkIpSource.allowPostRequestsFromIP, 
 //   limitRequestsByIp,
    limitResultSize.elasticSearchResultSizeLimiter,
    proxy('ll-es:9200')

]);

app.get('/', (req, res) => {
    res.send('hello world!');
});

app.listen(80, () => {
    console.log("listening on port 80.");
});