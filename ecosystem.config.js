module.exports = {
  apps: [
    {
      name: "crossroad-api",
      script: "/home/pranjal.p/miniconda3/envs/crossroad/bin/uvicorn",
      args: "crossroad.api.main:app --host 0.0.0.0 --port 8000",
      cwd: "/home/pranjal.p/preeti/croSSRoad",
      interpreter: "none",
      env: {
        CROSSROAD_ROOT: "/home/pranjal.p/preeti/croSSRoad",
        PATH: "/home/pranjal.p/miniconda3/envs/crossroad/bin:/home/pranjal.p/miniconda3/bin:/usr/local/bin:/usr/bin:/bin",
      },
      error_file: "/home/pranjal.p/.pm2/logs/crossroad-api-error.log",
      out_file: "/home/pranjal.p/.pm2/logs/crossroad-api-out.log",
      log_date_format: "YYYY-MM-DD HH:mm:ss",
      restart_delay: 5000,
      max_restarts: 5,
    },
  ],
};
