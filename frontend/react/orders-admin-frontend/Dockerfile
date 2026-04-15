FROM node:20-alpine AS builder

WORKDIR /app

COPY package.json package-lock.json ./
RUN npm ci

COPY tsconfig*.json ./
COPY vite.config.ts ./
COPY index.html ./
COPY public ./public
COPY src ./src

RUN npm run build

FROM nginx:1.27-alpine

COPY --from=builder /app/dist /usr/share/nginx/html

RUN rm -f /etc/nginx/conf.d/default.conf && cat > /etc/nginx/conf.d/default.conf <<'EOF'
server {
    listen 80;
    server_name localhost;
    root /usr/share/nginx/html;
    index index.html;

    gzip on;
    gzip_types text/plain text/css application/json application/javascript text/javascript application/xml image/svg+xml;

    location / {
        try_files $uri $uri/ /index.html;
    }
}
EOF

EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]
