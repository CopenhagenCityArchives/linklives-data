FROM node:12

# Create app directory
WORKDIR /usr/src/app

RUN npm install -g nodemon

RUN npm install

EXPOSE 80