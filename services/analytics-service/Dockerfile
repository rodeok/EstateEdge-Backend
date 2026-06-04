FROM node:20-alpine
WORKDIR /app

COPY shared/package*.json ./shared/
RUN cd shared && npm install

COPY shared/ ./shared/

COPY services/analytics-service/package*.json ./services/analytics-service/
RUN cd services/analytics-service && npm install

COPY services/analytics-service/ ./services/analytics-service/

RUN cd services/analytics-service && npm run build

WORKDIR /app/services/analytics-service
ENV NODE_ENV=production
EXPOSE 4004
CMD ["node", "dist/services/analytics-service/index.js"]
