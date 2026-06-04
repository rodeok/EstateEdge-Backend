FROM node:20-alpine
WORKDIR /app

# Copy shared first (services depend on it)
COPY shared/ ./shared/
RUN cd shared && npm install

COPY gateway/package*.json ./gateway/
RUN cd gateway && npm install

COPY gateway/ ./gateway/

RUN cd gateway && npm run build

WORKDIR /app/gateway
ENV NODE_ENV=production
EXPOSE 4000
CMD ["node", "dist/index.js"]
