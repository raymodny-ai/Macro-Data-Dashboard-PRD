FROM node:20-alpine

WORKDIR /app

# 安装依赖
COPY package.json ./
RUN npm install

# 复制源代码
COPY . .

# 开发模式启动 (生产环境使用 npm run build + nginx)
EXPOSE 5173

CMD ["npm", "run", "dev"]
