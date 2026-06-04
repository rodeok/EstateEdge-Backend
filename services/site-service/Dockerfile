FROM node:20-alpine
WORKDIR /app

COPY shared/ ./shared/
RUN cd shared && npm install

COPY services/site-service/package*.json ./services/site-service/
RUN cd services/site-service && npm install

COPY services/site-service/ ./services/site-service/

RUN cd services/site-service && npm run build

WORKDIR /app/services/site-service
ENV NODE_ENV=production
EXPOSE 4001
CMD ["node", "dist/index.js"]
