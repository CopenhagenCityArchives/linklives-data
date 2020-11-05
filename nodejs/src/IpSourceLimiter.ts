import express = require('express');

export function allowPostRequestsFromIP(req: express.Request, res: express.Response, next: any) : void {
    console.log("ipsourcelimiter");
    if(req.method == 'POST' && req.ip != '82.129.168.12'){
        res.status(400);
        
        res.send("Forbidden");
    }
    else{
        next();
    }
}
