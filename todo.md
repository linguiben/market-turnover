1. 右上角 "LAST UPDATED: 2026-02-10 16:00" 修改为 "Last Data Sync \n2026-02-10 16:00:00"(文字和日期之间分行)
2. 增加10日均值
3. 页面下方增加 "
智能行情分析 (Insights)
"当前恒生指数全日成交量接近历史峰值的70%，显示出极强的底部支撑力度。相比之下，上证指数成交量略低于5日均值，处于短期震荡整理阶段，建议关注能否突破上方压力位。"

67%
HSI vs MAX
73%
SSE vs MAX"
4. 卡片中已有一条虚线横线显示历史最高的全日成交量（文字显示在右边），增加一条显示历史最高的半日成交量（文字显示在左边），颜色稍微淡一些，与全日的横线作区分。
5. 卡片中指数价格的旁边增加上线红/绿上下箭头和升降的百分比。


• 可以，按下面这套就能 不走 Docker 本地调试，并且可直接登录数据库。

  1) 准备本地 PostgreSQL

  - 启动本地 PG（mac 常见）：brew services start postgresql@16
  - 创建用户和库（示例）：
      - psql postgres -c "CREATE USER mt_user WITH PASSWORD 'mt_pass';"
      - psql postgres -c "CREATE DATABASE market_turnover OWNER mt_user;"

  2) 在项目根目录创建 .env

  - 这个仓库当前没有 .env.example，你手动新建 .env 即可。
  - 最小配置建议：
      - DATABASE_URL=postgresql+psycopg://mt_user:mt_pass@127.0.0.1:5432/
        market_turnover
      - BASE_PATH=/market-turnover（若想根路径访问可设为空）
      - TZ=Asia/Shanghai
      - AASTOCKS_TIMEOUT_SECONDS=10
      - HKEX_TIMEOUT_SECONDS=20

  3) 安装依赖并迁移
  - source .venv/bin/activate
  - pip install -r requirements.txt
  - alembic upgrade head
  - 访问：
      - http://localhost:8000/healthz
      - http://localhost:8000/market-turnover

  5) 登录数据库验证

  - PGPASSWORD=mt_pass psql -h 127.0.0.1 -p 5432 -U mt_user -d market_turnover
  - 进库后可看表：\dt

######## loca测试方式 ######
1. Docker 方式

  docker compose up -d --build
  docker compose ps
  docker compose logs --tail=100 web

  2. 本地 venv 方式

  source .venv/bin/activate
  uvicorn app.main:app --host 0.0.0.0 --port 8000

  启动后先测：

  curl -i http://localhost:8000/healthz
  curl -i http://localhost:8000/market-turnover/healthz

  再打开：
  http://localhost:8000/market-turnover/（建议带末尾 /）。

###### 执行数据抓取任务 ######
• 现在可以通过 POST 接口触发（当前 Jobs 页面是只读列表，没有按
  钮）。
  1. 先确保服务已启动

  docker compose up -d --build
  2. 触发任务（任选）

  # 午盘抓取
  curl -X POST -F 'job_name=fetch_am' http://localhost:8000/market-turnover/api/jobs/run

  # 全日抓取
  curl -X POST -F 'job_name=fetch_full' http://localhost:8000/market-turnover/api/jobs/run
  # 同步指数（日线，Tushare）
  curl -X POST -F 'job_name=fetch_tushare_index' http://localhost:8000/market-turnover/api/jobs/run

  # 回填HKEX历史
  curl -X POST -F 'job_name=backfill_hkex' http://localhost:8000/market-turnover/api/jobs/run
  # from https://www.hkex.com.hk/Market-Data/Statistics/Consolidated-Reports/
  # 回填SSE历史
  curl -X POST -F 'job_name=backfill_tushare_index' http://localhost:8000/market-turnover/api/jobs/run

  3. 查看执行结果
     打开：http://localhost:8000/market-turnover/jobs

  建议先跑：backfill_hkex -> fetch_tushare_index -> fetch_full。