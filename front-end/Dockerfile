# Requires @sveltejs/adapter-node: adapter-auto produces no runnable server.
FROM node:24-alpine AS build

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY . .
RUN npm run build

FROM node:24-alpine

WORKDIR /app

ENV NODE_ENV=production

COPY package.json package-lock.json ./
RUN npm ci --omit=dev

COPY --from=build /app/build ./build

# adapter-node reads PORT/HOST at runtime.
ENV PORT=3000 \
    HOST=0.0.0.0

CMD ["node", "build"]
