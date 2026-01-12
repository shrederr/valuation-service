FROM node:20-alpine AS builder

WORKDIR /app

COPY package.json yarn.lock ./
RUN yarn install --frozen-lockfile

COPY . .
RUN yarn build

# Compile migrations separately (webpack bundles app, but migrations need to be separate)
RUN npx tsc db/migrations/*.ts db/datasource.ts --outDir dist/db --esModuleInterop --module commonjs --target ES2020 --skipLibCheck

# Debug: list migration files to verify compilation
RUN ls -la dist/db/migrations/ || echo "No migrations found"

FROM node:20-alpine AS production

WORKDIR /app

COPY --from=builder /app/package.json /app/yarn.lock ./
COPY --from=builder /app/dist ./dist
# Explicitly ensure migrations are copied (compiled in dist/db/)
COPY --from=builder /app/dist/db ./dist/db
COPY --from=builder /app/node_modules ./node_modules
COPY --from=builder /app/public ./public

ENV NODE_ENV=production
ENV PORT=3000

EXPOSE 3000

CMD ["node", "dist/apps/valuation/main.js"]
