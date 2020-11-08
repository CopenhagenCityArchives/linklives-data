import express from 'express';

interface IpsMethods {
    ip: string;
    allowedMethods: Array<string>;
}

function LimitRequestTypesAndIps(allowedIpsAndMethods: Array<IpsMethods>){
    function checkMethodAndIp(req: express.Request, res: express.Response, next: any) : void {
        allowedIpsAndMethods.forEach(element => {
            if((element.ip == req.ip || element.ip == '*') && element.allowedMethods.indexOf(req.method)){
                next();
            }
        });

        res.status(403);
        res.send("Forbidden");
    }

    return checkMethodAndIp;
}

export {LimitRequestTypesAndIps, IpsMethods};


