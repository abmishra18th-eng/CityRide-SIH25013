from fastapi import FastAPI, WebSocket, WebSocketDisconnect, HTTPException, Query
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from pathlib import Path
from datetime import datetime, timezone
import sqlite3
import asyncio
import math
import threading
import os

BASE = Path(__file__).parent
DB = BASE / "cityride.db"
STALE_SECONDS = int(os.getenv("STALE_SECONDS", "20"))

app = FastAPI(title="CityRide SIH25013 V4", version="4.3")
app.mount("/static", StaticFiles(directory=BASE / "static"), name="static")

# Demo city data. Replace/extend with official GTFS/GTFS-Realtime data in deployment.
ROUTES = {
    "R12": {"name": "Railway Station ↔ Bus Stand", "stops": [
        {"id":"S1","name":"Railway Station","lat":30.70465,"lng":76.71787},
        {"id":"S2","name":"Sector 17","lat":30.73983,"lng":76.78270},
        {"id":"S3","name":"Civil Hospital","lat":30.70490,"lng":76.79360},
        {"id":"S4","name":"University Gate","lat":30.76190,"lng":76.76870},
        {"id":"S5","name":"Bus Stand","lat":30.71960,"lng":76.83750},
    ]},
    "R18": {"name": "Railway Station ↔ University", "stops": [
        {"id":"S6","name":"Railway Station","lat":30.70465,"lng":76.71787},
        {"id":"S7","name":"Market Chowk","lat":30.71350,"lng":76.74520},
        {"id":"S8","name":"Hospital Road","lat":30.73110,"lng":76.75800},
        {"id":"S9","name":"University Gate","lat":30.76190,"lng":76.76870},
    ]},
}

BUSES = {
    "PB10-1001": {"route_id":"R12","lat":30.70465,"lng":76.71787,"speed":32,"active":False,"crowd":32,"status":"Waiting","updated":None,"source":"demo","demo_index":0},
    "PB10-1002": {"route_id":"R18","lat":30.70465,"lng":76.71787,"speed":28,"active":False,"crowd":54,"status":"Waiting","updated":None,"source":"demo","demo_index":0},
    "PB10-1003": {"route_id":"R12","lat":30.71960,"lng":76.83750,"speed":24,"active":True,"crowd":71,"status":"On Time","updated":None,"source":"demo","demo_index":3},
}

class GPSUpdate(BaseModel):
    bus_id: str
    route_id: str
    lat: float
    lng: float
    speed: float = 0
    crowd: int = 0

class TripAction(BaseModel):
    bus_id: str
    route_id: str

class Hub:
    def __init__(self): self.clients = set()
    async def connect(self, ws): await ws.accept(); self.clients.add(ws)
    def disconnect(self, ws): self.clients.discard(ws)
    async def broadcast(self, payload):
        dead=[]
        for ws in list(self.clients):
            try: await ws.send_json(payload)
            except Exception: dead.append(ws)
        for ws in dead: self.clients.discard(ws)

hub=Hub(); lock=threading.Lock()

def db():
    conn=sqlite3.connect(DB)
    conn.execute("CREATE TABLE IF NOT EXISTS locations (id INTEGER PRIMARY KEY AUTOINCREMENT,bus_id TEXT,route_id TEXT,lat REAL,lng REAL,speed REAL,crowd INTEGER,source TEXT,created_at TEXT)")
    conn.execute("CREATE TABLE IF NOT EXISTS trips (id INTEGER PRIMARY KEY AUTOINCREMENT,bus_id TEXT,route_id TEXT,started_at TEXT,ended_at TEXT,status TEXT)")
    conn.commit(); return conn

def now(): return datetime.now(timezone.utc).isoformat()
def parse_dt(v):
    if not v: return None
    try: return datetime.fromisoformat(v.replace('Z','+00:00'))
    except Exception: return None

def distance_km(a_lat,a_lng,b_lat,b_lng):
    r=6371.0; p1,p2=math.radians(a_lat),math.radians(b_lat); dp=math.radians(b_lat-a_lat); dl=math.radians(b_lng-a_lng)
    x=math.sin(dp/2)**2+math.cos(p1)*math.cos(p2)*math.sin(dl/2)**2
    return 2*r*math.asin(math.sqrt(x))

def crowd_label(c): return "Low" if c<40 else ("Medium" if c<70 else "High")
def nearest_stop(bus,route): return min(route["stops"], key=lambda s: distance_km(bus["lat"],bus["lng"],s["lat"],s["lng"]))
def calculate_eta_minutes(bus,stop):
    d=distance_km(bus["lat"],bus["lng"],stop["lat"],stop["lng"]); speed=max(float(bus.get("speed",20)),8)
    return max(1,round((d/speed)*60))

def is_stale(b):
    t=parse_dt(b.get("updated")); return bool(t and (datetime.now(timezone.utc)-t).total_seconds()>STALE_SECONDS)

def public_state():
    result=[]
    with lock:
        for bid,b in BUSES.items():
            route=ROUTES[b["route_id"]]; ns=nearest_stop(b,route); stale=is_stale(b)
            status="Offline" if stale and b["active"] else b["status"]
            result.append({"bus_id":bid,"route_id":b["route_id"],"route_name":route["name"],"lat":b["lat"],"lng":b["lng"],"speed":round(b["speed"],1),"active":b["active"],"stale":stale,"crowd":b["crowd"],"crowd_label":crowd_label(b["crowd"]),"status":status,"next_stop":ns["name"],"eta":calculate_eta_minutes(b,ns),"updated":b["updated"],"source":b.get("source","demo")})
    return result

db().close()

@app.get("/")
async def home(): return FileResponse(BASE/"static"/"index.html")
@app.get("/api/routes")
async def routes(): return ROUTES
@app.get("/api/buses")
async def buses(): return {"buses":public_state(),"server_time":now()}
@app.get("/api/nearby")
async def nearby(lat: float=Query(...), lng: float=Query(...), radius_km: float=Query(5,ge=0.1,le=100)):
    items=[]
    for b in public_state():
        d=distance_km(lat,lng,b["lat"],b["lng"]); b["distance_km"]=round(d,2)
        if d<=radius_km: items.append(b)
    items.sort(key=lambda x:x["distance_km"])
    return {"buses":items,"count":len(items)}

@app.post("/api/trip/start")
async def start_trip(action:TripAction):
    if action.bus_id not in BUSES or action.route_id not in ROUTES: raise HTTPException(404,"Unknown bus or route")
    with lock:
        b=BUSES[action.bus_id]; b.update(route_id=action.route_id,active=True,status="On Time",updated=now(),source="phone_pending")
    conn=db(); conn.execute("INSERT INTO trips(bus_id,route_id,started_at,status) VALUES(?,?,?,?)",(action.bus_id,action.route_id,now(),"active")); conn.commit(); conn.close()
    await hub.broadcast({"type":"fleet_update","buses":public_state()}); return {"ok":True,"message":"Trip started"}

@app.post("/api/trip/end")
async def end_trip(action:TripAction):
    if action.bus_id not in BUSES: raise HTTPException(404,"Unknown bus")
    with lock: BUSES[action.bus_id].update(active=False,status="Waiting",updated=now())
    conn=db(); conn.execute("UPDATE trips SET ended_at=?,status='completed' WHERE bus_id=? AND ended_at IS NULL",(now(),action.bus_id)); conn.commit(); conn.close()
    await hub.broadcast({"type":"fleet_update","buses":public_state()}); return {"ok":True,"message":"Trip ended"}

@app.post("/api/gps")
async def gps(update:GPSUpdate):
    if update.bus_id not in BUSES: raise HTTPException(404,"Unknown bus")
    if not (-90<=update.lat<=90 and -180<=update.lng<=180): raise HTTPException(400,"Invalid coordinates")
    timestamp=now()
    with lock:
        b=BUSES[update.bus_id]; b.update(route_id=update.route_id,lat=update.lat,lng=update.lng,speed=max(0,update.speed),crowd=max(0,min(100,update.crowd)),active=True,status="On Time" if update.speed>=12 else "Delayed",updated=timestamp,source="phone-gps")
    conn=db(); conn.execute("INSERT INTO locations(bus_id,route_id,lat,lng,speed,crowd,source,created_at) VALUES(?,?,?,?,?,?,?,?)",(update.bus_id,update.route_id,update.lat,update.lng,update.speed,update.crowd,"phone-gps",timestamp)); conn.commit(); conn.close()
    await hub.broadcast({"type":"fleet_update","buses":public_state()}); return {"ok":True,"received_at":timestamp}

@app.post("/api/demo/start")
async def demo_start(action:TripAction):
    if action.bus_id not in BUSES or action.route_id not in ROUTES: raise HTTPException(404,"Unknown bus or route")
    with lock: BUSES[action.bus_id].update(route_id=action.route_id,active=True,status="On Time",demo_index=0,source="demo",updated=now())
    return {"ok":True}

@app.get("/api/stats")
async def stats():
    bs=public_state(); return {"active":sum(b["active"] for b in bs),"delayed":sum(b["status"]=="Delayed" for b in bs),"offline":sum(b["status"]=="Offline" for b in bs),"high_crowd":sum(b["crowd_label"]=="High" for b in bs),"total":len(bs)}

@app.get("/api/integrations")
async def integrations():
    return {"version":"4.3","live_phone_gps":True,"websocket":True,"passenger_location":True,"nearby_buses":True,"gtfs_realtime":{"configured":bool(os.getenv('GTFS_RT_VEHICLE_POSITIONS_URL')),"url_configured":bool(os.getenv('GTFS_RT_VEHICLE_POSITIONS_URL'))},"train_status":{"configured":bool(os.getenv('TRAIN_STATUS_API_URL'))},"note":"External bus/train feeds require an authorized/public provider endpoint. The demo never pretends simulated data is live external data."}

@app.websocket("/ws")
async def websocket_endpoint(ws:WebSocket):
    await hub.connect(ws)
    try:
        await ws.send_json({"type":"fleet_update","buses":public_state()})
        while True: await ws.receive_text()
    except (WebSocketDisconnect,Exception): hub.disconnect(ws)

def move_demo_bus():
    for bid,b in BUSES.items():
        if not b["active"] or b.get("source")!="demo": continue
        stops=ROUTES[b["route_id"]]["stops"]; i=b["demo_index"]%len(stops); j=(i+1)%len(stops); a,z=stops[i],stops[j]; step=.07
        b["lat"]+=(z["lat"]-b["lat"])*step; b["lng"]+=(z["lng"]-b["lng"])*step; b["speed"]=18+((i*7)%20); b["crowd"]=25+((i*19+len(bid))%70); b["status"]="Delayed" if b["speed"]<20 else "On Time"; b["updated"]=now()
        if distance_km(b["lat"],b["lng"],z["lat"],z["lng"])<.08: b["demo_index"]=j

async def simulator():
    while True:
        await asyncio.sleep(2)
        with lock: move_demo_bus()
        await hub.broadcast({"type":"fleet_update","buses":public_state()})

@app.on_event("startup")
async def startup(): asyncio.create_task(simulator())
