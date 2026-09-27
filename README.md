# CityRide SIH25013 V4.3

This version fixes the V4.2 frontend JavaScript syntax error that prevented Passenger/Driver/Authority navigation and location code from running.

## Run on Windows

```powershell
cd CityRide_SIH25013_V4_3
python -m pip install -r requirements.txt
python -m uvicorn main:app --reload
```

Open:

`http://127.0.0.1:8000`

## Test order

1. Passenger, Driver and Authority navigation should respond immediately.
2. Driver → Start Demo GPS should move demo data.
3. Passenger → Use My Current Location should request browser location permission on localhost.
4. Driver → Start Live Phone GPS should send the phone's GPS to `/api/gps`.

Do not deploy publicly until these local tests work.
