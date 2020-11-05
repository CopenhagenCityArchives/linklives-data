import { expect } from 'chai';
import express = require('express');

import { Request, Response, NextFunction } from 'express';

import { elasticSearchResultSizeLimiter } from '../src/ResultSizeLimiter';
describe("ElasticSearchResultSizeLimiter tests", () => {
    describe("Limit size if set", () =>  {
     it('should limit size of body if set', () => {
        const mockRequest = {
            body: {
                size: 1
            },
        } as Request;


        const mockResponse = {} as Response; 

        const mockNext = () => {} as NextFunction;

        elasticSearchResultSizeLimiter(mockRequest, mockResponse, mockNext);

        expect(mockRequest.body.size).to.exist;

        expect(mockRequest.body.size).to.equal(1);
     });    
    });

});