FROM node:20-alpine
WORKDIR /app

COPY shared/package*.json ./shared/
RUN cd shared && npm install

COPY shared/ ./shared/

COPY services/user-service/package*.json ./services/user-service/
RUN cd services/user-service && npm install

COPY services/user-service/ ./services/user-service/

RUN cd services/user-service && npm run build

WORKDIR /app/services/user-service
ENV NODE_ENV=production
EXPOSE 4003
CMD ["node", "dist/services/user-service/index.js"]
