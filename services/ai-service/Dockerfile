FROM node:20-alpine
WORKDIR /app

COPY shared/ ./shared/
RUN cd shared && npm install

COPY services/ai-service/package*.json ./services/ai-service/
RUN cd services/ai-service && npm install

COPY services/ai-service/ ./services/ai-service/

RUN cd services/ai-service && npm run build

WORKDIR /app/services/ai-service
ENV NODE_ENV=production
EXPOSE 4002
CMD ["node", "dist/src/index.js"]
