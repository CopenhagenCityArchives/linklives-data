import { expect } from 'chai';
import { Request, Response, NextFunction } from 'express';
import ElasticSearchResultSizeLimiter from '../src/ElasticSearchResultSizeLimiter';

describe("ElasticSearchResultSizeLimiter tests", () => {
    describe("alter size parameter in body", () =>  {
     it('should limit size of body if set', () => {
        const mockRequest = {
            body: {
                size: 10
            },
        } as Request;

        const mockResponse = {} as Response; 

        const mockNext = () => {};

        ElasticSearchResultSizeLimiter(mockRequest, mockResponse, mockNext);

        expect(mockRequest.body.size).to.exist;

        expect(mockRequest.body.size).to.equal(1);
     });   
     
     it('should set size of body to threshold value if not set', () => {
        const mockRequest = {
            body: {
            },
        } as Request;

        const mockResponse = {} as Response; 

        const mockNext = () => {};

        ElasticSearchResultSizeLimiter(mockRequest, mockResponse, mockNext);

        expect(mockRequest.body.size).to.exist;

        expect(mockRequest.body.size).to.equal(1);
     });

     it('should not limit size of body if it is beneath threshold', () => {
        const mockRequest = {
            body: {
                size: 3
            },
        } as Request;

        const mockResponse = {} as Response; 

        const mockNext = () => {};

        ElasticSearchResultSizeLimiter(mockRequest, mockResponse, mockNext);

        expect(mockRequest.body.size).to.exist;

        expect(mockRequest.body.size).to.equal(3);
     });

     it('should limit size of body if existing size is less than zero', () => {
        const mockRequest = {
            body: {
                size: -3
            },
        } as Request;

        const mockResponse = {} as Response; 

        const mockNext = () => {};

        ElasticSearchResultSizeLimiter(mockRequest, mockResponse, mockNext);

        expect(mockRequest.body.size).to.exist;

        expect(mockRequest.body.size).to.equal(10);
     });
    });

});