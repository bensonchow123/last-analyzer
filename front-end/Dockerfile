# Requires @sveltejs/adapter-node: adapter-auto produces no runnable server.
# The dev stage carries the full toolchain; docker-compose.dev.yaml targets it
# and bind-mounts the source over /app for hot reload.
FROM node:24-alpine AS dev

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY . .

# fallback to dev
CMD ["npm", "run", "dev"]

FROM dev AS build

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
