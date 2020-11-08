"use strict";
Object.defineProperty(exports, "__esModule", { value: true });
exports.elasticSearchResultSizeLimiter = void 0;
function elasticSearchResultSizeLimiter(req, res, next) {
    var _a;
    if (req.method == 'GET' && !(req === null || req === void 0 ? void 0 : req.body.size)) {
        console.log("limiting body size");
        req.body.size = 1;
    }
    if (req.method == 'GET' && ((_a = req === null || req === void 0 ? void 0 : req.body) === null || _a === void 0 ? void 0 : _a.size) > 1000) {
        console.log("setting body size");
        req.body.size = 1;
    }
    next();
}
exports.elasticSearchResultSizeLimiter = elasticSearchResultSizeLimiter;
