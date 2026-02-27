# Deploy config for portfolio-sasp

## 1) Install systemd service
```bash
sudo cp /home/gabo/portfolio/projects/05-sasp/desploy/portfolio-sasp.service /etc/systemd/system/portfolio-sasp.service
sudo cp /home/gabo/portfolio/projects/05-sasp/desploy/portfolio-sasp.env.example /etc/default/portfolio-sasp
sudo systemctl daemon-reload
sudo systemctl enable --now portfolio-sasp
```

## 2) Install nginx vhost
```bash
sudo cp /home/gabo/portfolio/projects/05-sasp/desploy/portfolio-sasp.nginx /etc/nginx/sites-available/portfolio-sasp.conf
sudo ln -s /etc/nginx/sites-available/portfolio-sasp.conf /etc/nginx/sites-enabled/portfolio-sasp
sudo nginx -t
sudo systemctl reload nginx
```

## 3) Validate service
```bash
sudo systemctl status portfolio-sasp --no-pager -l
sudo journalctl -u portfolio-sasp -n 100 --no-pager
```

## Notes
- This service uses Gunicorn from project venv: `/home/gabo/portfolio/projects/05-sasp/venv/bin/gunicorn`.
- DB path is fixed via `SCIL_DB=/home/gabo/portfolio/projects/05-sasp/scil.db`.
- App reads `SECRET_KEY` from environment if set.
