# --- build stage: compile TypeScript, including migrations/data-source ---
FROM node:20-alpine AS build
WORKDIR /app
COPY package*.json ./
RUN npm ci
COPY . .
RUN npm run build

# --- production stage: lean image, only prod deps + compiled output ---
FROM node:20-alpine AS production
WORKDIR /app
ENV NODE_ENV=production
# psql/createdb — the target database doesn't exist until something creates
# it; Postgres never does this on its own, and TypeORM migrations only
# create tables inside a database that already exists, not the database
# itself. docker-entrypoint.sh creates it first if missing.
RUN apk add --no-cache postgresql-client
COPY package*.json ./
RUN npm ci --omit=dev
COPY --from=build /app/dist ./dist
COPY docker-entrypoint.sh ./
RUN chmod +x docker-entrypoint.sh

EXPOSE 3000

# Ensures the database exists, runs pending migrations against dist's
# compiled data-source (no ts-node/typescript needed at runtime — those are
# dev-only), then starts the app. Both steps are idempotent — a restart
# with nothing new to do is a fast no-op before node dist/main starts.
CMD ["./docker-entrypoint.sh"]
