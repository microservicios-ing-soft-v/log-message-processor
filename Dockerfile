FROM node:14-alpine

WORKDIR /app

COPY package*.json ./
RUN npm install

COPY . .

ENV REDIS_HOST=localhost
ENV REDIS_PORT=6379
ENV REDIS_CHANNEL=log_channel
ENV ZIPKIN_URL=http://localhost:9411/api/v2/spans

CMD ["node", "index.js"]