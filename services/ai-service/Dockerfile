FROM node:20-alpine
WORKDIR /app

# shared deps
COPY shared/package*.json ./shared/
RUN cd shared && npm install

COPY shared/ ./shared/

# ai-service deps
COPY services/ai-service/package*.json ./services/ai-service/
RUN cd services/ai-service && npm install

COPY services/ai-service/ ./services/ai-service/

RUN cd services/ai-service && npm run build

WORKDIR /app/services/ai-service
ENV NODE_ENV=production
EXPOSE 4002
# rootDir=../../ means dist mirrors full path: dist/services/ai-service/src/index.js
CMD ["node", "dist/services/ai-service/src/index.js"]
