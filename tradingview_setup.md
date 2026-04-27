# TradingView Setup

## الطريقة المختصرة: Scanner واحد لكل الأسهم

1. افتح أي شارت في TradingView.
2. افتح Pine Editor.
3. الصق سكربت:
   `pine/UAE_Core_DataFeed_And_Signals_V1.pine`
4. اضغط Add to chart.
5. من Settings غيّر Secret إلى نفس SECRET في ملف main.py.
6. Create Alert.
7. Condition:
   `UAE Core DataFeed + Signals V1` ثم `Any alert() function call`.
8. Trigger:
   `Once per bar close`.
9. Webhook URL:
```text
https://YOUR-NGROK.ngrok-free.app/webhook/tradingview
```
10. Notifications:
- Webhook ON
- App OFF
- Email OFF
- Sound OFF

السكربت يرسل الداتا والإشارات برسائل JSON ديناميكية، فلا تحتاج تكتب Message يدوي.
