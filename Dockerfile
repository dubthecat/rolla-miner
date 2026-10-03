# ghcr.io/dubthecat/rolla-miner — one L3 miner. Builds the native book (C++20) and runs src/run.mjs.
FROM node:22-bookworm-slim
RUN apt-get update && apt-get install -y --no-install-recommends build-essential make ca-certificates curl && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY package.json package-lock.json* ./
RUN npm install --omit=dev --no-audit --no-fund
COPY . .
RUN make -C native/book libbook.so bookd trace && make -C native/book test >/tmp/native-test.log 2>&1 || (tail -n 20 /tmp/native-test.log; exit 1)
ENV NODE_ENV=production L3_NATIVE=1 PORT=8080
EXPOSE 8080
CMD ["node", "src/run.mjs"]
