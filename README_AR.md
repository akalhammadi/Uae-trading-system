# UAE Trading System Starter

## التشغيل
1. ثبت المتطلبات:
```bash
pip install -r requirements.txt
```

2. شغل السيرفر:
```bash
uvicorn main:app --host 0.0.0.0 --port 8000
```

3. افتح السيرفر للإنترنت عبر ngrok:
```bash
ngrok http 8000
```

4. استخدم رابط ngrok في TradingView Webhook:
```text
https://YOUR-NGROK.ngrok-free.app/webhook/tradingview
```

## Endpoints
- POST /webhook/tradingview
- GET /api/candles/latest
- GET /api/signals/latest
- GET /api/dashboard
