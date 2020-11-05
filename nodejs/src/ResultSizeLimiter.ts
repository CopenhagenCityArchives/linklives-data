import express = require('express');

export function elasticSearchResultSizeLimiter(req: express.Request, res: express.Response, next: any) : void {
    if(req.method == 'GET' && !req?.body.size){
        console.log("limiting body size");
        req.body.size = 1;
    }

    if(req.method == 'GET' && req?.body?.size > 1){
        console.log("setting body size");
        req.body.size = 1;
    }

    next();
}
