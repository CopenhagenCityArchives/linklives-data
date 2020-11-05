"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.allowPostRequestsFromIP = void 0;
function allowPostRequestsFromIP(req, res, next) {
    if (req.method == 'POST' && req.ip != '82.129.168.12') {
        res.status(400);
        res.send("Forbidden");
    }
    else {
        next();
    }
}
exports.allowPostRequestsFromIP = allowPostRequestsFromIP;
