import express = require('express');
import { proxy } from ('express-http-proxy');
import { rateLimit } from ("express-rate-limit");
import { ResultSizeLimiter } from ('./ResultSizeLimiter');
import { LimitRequestTypesAndIps, IpsMethods} from "./LimitRequestTypesAndIps";

const app : express.Application = express();

// Enable if you're behind a reverse proxy (Heroku, Bluemix, AWS ELB, Nginx, etc)
// see https://expressjs.com/en/guide/behind-proxies.html
app.set('trust proxy', 1);

const limitRequestsByIp = rateLimit({
    windowMs: 15 * 60 * 1000, // 15 minutes
    max: 5 // limit each IP to 100 requests per windowMs
  }); 

const elasticSearchSafeRequests = LimitRequestTypesAndIps([
    {
        ip: '{HOME_IP}',
        allowedMethods: ['post', 'delete', 'options', 'get']
    },
    {
        ip: '*',
        allowedMethods: ['get', 'options']
    }
]);
  
// Decode JSON in request body
app.use(express.json());


app.use([ 
    elasticSearchSafeRequests,  // Limit post requests to safe ip and allow get and options from all other
    proxy('ll-es:9200')         // Proxy requests to Elasticsearch
]);

app.get(['pas','links','lifecourses'],
    limitRequestsByIp,          // Limit requests by ip
    ResultSizeLimiter,          // Always set size limit
);

app.get('/', (req: express.Request, res: express.Response) => {
    res.send('hello world!');
});

app.listen(80, () => {
    console.log("listening on port 80.");
});