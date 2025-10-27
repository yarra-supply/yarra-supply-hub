
# 本地开发
cp infra/.env.example infra/.env
cp backend/.env.example backend/.env.dev
docker compose -f infra/docker-compose.yml up -d

