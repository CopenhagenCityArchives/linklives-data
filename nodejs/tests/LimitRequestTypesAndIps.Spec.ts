import express = require('express');
import LimitRequestTypesAndIps from "../src/LimitRequestTypesAndIps";
import { sinon } from 'sinon';
import { Request, Response } from 'express';


describe('LimitRequestTypesAndIpsTests', () => {
    it('should allow ips when using *', () => {

        const allowAllGetRequests = LimitRequestTypesAndIps([
            {
                ip: '*',
                allowedMethods: ['get']
            }
        ]);

        const mockRequest = {
            method: "get",
            ip: "192.168.0.1"
        } as Request;

        const mockResponse = {} as Response; 
        const mockNext =  sinon.spy();

        allowAllGetRequests(mockRequest, mockResponse, mockNext);
        expect(mockNext.called).to.be(true);
    })

    it('should allow ips when using a concrete ip', () => {

        const allowAllGetRequests = LimitRequestTypesAndIps([
            {
                ip: '192.168.0.1',
                allowedMethods: ['get']
            }
        ]);

        const mockRequest = {
            method: "get",
            ip: "192.168.0.1"
        } as Request;

        const mockResponse = {} as Response; 
        const mockNext =  sinon.spy();

        allowAllGetRequests(mockRequest, mockResponse, mockNext);
        expect(mockNext.called).to.be(true);
    })

    it('should skip ips if not matched', () => {

        const allowAllGetRequests = LimitRequestTypesAndIps([
            {
                ip: '192.168.0.1',
                allowedMethods: ['get']
            }
        ]);

        const mockRequest = {
            method: "get",
            ip: "87.65.44.23"
        } as Request;

        const mockResponse = {} as Response; 
        const mockNext =  sinon.spy();

        allowAllGetRequests(mockRequest, mockResponse, mockNext);
        
        expect(mockNext.called).to.be(false);
        expect(mockResponse.status).to.be(403);
    })

    it('should not allow methods if not matched', () => {

        const allowAllGetRequests = LimitRequestTypesAndIps([
            {
                ip: '*',
                allowedMethods: ['get']
            }
        ]);

        const mockRequest = {
            method: "post",
            ip: "87.65.44.23"
        } as Request;

        const mockResponse = {} as Response; 
        const mockNext =  sinon.spy();

        allowAllGetRequests(mockRequest, mockResponse, mockNext);
        
        expect(mockNext.called).to.be(false);
        expect(mockResponse.status).to.be(403);
    })
});