"""
╔══════════════════════════════════════════════════════════╗
║   PB INTELLIGENCE SYSTEM v3.0 — The Ultimate Weapon     ║
║   ICT Mech Model · Real-Time · Continuous Scanner       ║
╠══════════════════════════════════════════════════════════╣
║   LAUNCH:  streamlit run app.py                          ║
║   DATA:    Tradovate (real-time) or yfinance (fallback)  ║
╚══════════════════════════════════════════════════════════╝
"""

import streamlit as st, pandas as pd, numpy as np, yfinance as yf
import plotly.graph_objects as go
from plotly.subplots import make_subplots
import feedparser, os, time, warnings
from datetime import datetime, timedelta
from datetime import time as dtime
import pytz
from vaderSentiment.vaderSentiment import SentimentIntensityAnalyzer
warnings.filterwarnings("ignore")

ET = pytz.timezone("America/New_York")

# ──────────────────────────────────────────────────────────────
# PAGE CONFIG
# ──────────────────────────────────────────────────────────────
st.set_page_config(page_title="PB Intelligence", page_icon="⚡", layout="wide")
st.markdown("""<style>
html,[class*="css"]{font-family:'Courier New',monospace}
.main{background:#05080f}
section[data-testid="stSidebar"]{background:#080c18;border-right:1px solid #111e33}
.block-container{padding-top:1rem;padding-bottom:0}
div[data-testid="metric-container"]{background:#0b1220;border:1px solid #111e33;border-radius:8px;padding:14px 16px}
div[data-testid="metric-container"] label{color:#3a5070!important;font-size:10px!important;letter-spacing:1.5px;text-transform:uppercase}
div[data-testid="metric-container"] div[data-testid="stMetricValue"]{font-size:20px!important;color:#e8f0ff!important}
.card{background:#0b1220;border:1px solid #111e33;border-radius:10px;padding:18px;margin-bottom:12px}
.ct{color:#2a5080;font-size:10px;font-weight:bold;letter-spacing:2px;text-transform:uppercase;margin-bottom:12px}
.sig-long{background:linear-gradient(135deg,#021508,#062e12);border:1.5px solid #16a34a;border-radius:12px;padding:22px;margin-bottom:12px}
.sig-short{background:linear-gradient(135deg,#150202,#2e0606);border:1.5px solid #dc2626;border-radius:12px;padding:22px;margin-bottom:12px}
.sig-wait{background:#0b1220;border:1px solid #111e33;border-radius:12px;padding:22px;margin-bottom:12px}
.row{display:flex;justify-content:space-between;padding:5px 0;border-bottom:1px solid #0d1520;font-size:12px}
.lv{color:#3a5070}.rv{font-family:monospace;color:#c8d8e8}
.pricebig{font-size:40px;font-weight:bold;font-family:'Courier New',monospace;color:#22c55e}
</style>""", unsafe_allow_html=True)

# ──────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────
C = dict(
    min_score=7.0, high_conviction=8.5, min_rr=1.5, target_rr=2.0,
    fvg_min=5, sweep_min=8, sweep_lb=20, smt_lb=10, atr_p=14,
    acct=50_000, risk_pct=0.01, max_day=3, tick_val=20,
    kill_zones={
        "London Open":{"s":dtime(2,0),"e":dtime(5,0),"w":0.8},
        "NY AM Open": {"s":dtime(8,30),"e":dtime(11,0),"w":1.0},
        "NY Lunch":   {"s":dtime(11,30),"e":dtime(13,0),"w":0.0},
        "NY PM":      {"s":dtime(13,0),"e":dtime(16,0),"w":0.7},
    },
    silver_bullets=[
        {"s":dtime(10,0),"e":dtime(11,0),"n":"NY Silver Bullet"},
        {"s":dtime(14,0),"e":dtime(15,0),"n":"PM Silver Bullet"},
    ],
    day_w={0:0.7,1:1.0,2:0.9,3:0.85,4:0.5,5:0.0,6:0.0},
    sc=dict(htf=2.0,kz=1.0,sweep=2.0,smt=2.0,pdz=1.0,news=0.5,sb_bonus=0.5,mss_bonus=0.5),
)
VADER = SentimentIntensityAnalyzer()

# ──────────────────────────────────────────────────────────────
# SESSION STATE
# ──────────────────────────────────────────────────────────────
for k,v in dict(tv_cli=None,tv_ok=False,page="signals",sigs=[],scans=0,bt=None).items():
    if k not in st.session_state: st.session_state[k]=v

# ──────────────────────────────────────────────────────────────
# DATA
# ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=30)
def yf_fetch(t,iv="5m",p="2d"):
    try:
        df=yf.Ticker(t).history(interval=iv,period=p)
        if df.empty: return pd.DataFrame()
        df.columns=[c.lower() for c in df.columns]
        df=df[["open","high","low","close","volume"]].dropna()
        if df.index.tzinfo: df.index=df.index.tz_convert(ET)
        return df
    except: return pd.DataFrame()

def get_nq(n=300):
    cli=st.session_state.tv_cli
    if cli and cli._connected and "NQ" in getattr(cli,"bars",{}):
        b=cli.bars["NQ"]
        if not b.empty: return b.tail(n)
    return yf_fetch("NQ=F").tail(n)

def get_es(n=300):
    cli=st.session_state.tv_cli
    if cli and cli._connected and "ES" in getattr(cli,"bars",{}):
        b=cli.bars["ES"]
        if not b.empty: return b.tail(n)
    return yf_fetch("ES=F").tail(n)

def live_price(sym):
    cli=st.session_state.tv_cli
    if cli:
        p=cli.get_live_price(sym)
        if p: return p
    df=get_nq(2) if sym=="NQ" else get_es(2)
    return float(df["close"].iloc[-1]) if not df.empty else None

def data_src():
    cli=st.session_state.tv_cli
    return "🟢 Tradovate LIVE" if (cli and cli._connected) else "🟡 yfinance (15-min delay)"

# ──────────────────────────────────────────────────────────────
# TRADOVATE CONNECT
# ──────────────────────────────────────────────────────────────
def tv_connect(user,pw,live=False):
    try:
        from tradovate import TradovateClient
        cli=TradovateClient(user,pw,live)
        ok=cli.authenticate()
        if ok:
            for sym in ["NQ","ES"]:
                bars=cli.get_bars(sym,"Minute",5,500)
                if not bars.empty: cli.bars[sym]=bars
            cli.start_streaming(["NQ","ES"])
            st.session_state.tv_cli=cli
            st.session_state.tv_ok=True
            return True,"Connected — real-time data active"
        return False,"Auth failed. Check username/password."
    except Exception as e:
        return False,f"Connection error: {e}"

# ──────────────────────────────────────────────────────────────
# ICT ANALYSIS
# ──────────────────────────────────────────────────────────────
def atr(df,p=14):
    if len(df)<p+1: return 20.0
    h,l,c=df["high"],df["low"],df["close"]
    tr=pd.concat([h-l,(h-c.shift()).abs(),(l-c.shift()).abs()],axis=1).max(axis=1)
    v=tr.rolling(p).mean().iloc[-1]
    return float(v) if not np.isnan(v) else 20.0

def bias(df):
    if len(df)<50: return {"bias":"neutral","str":0.5,"zone":"unknown","ema20":0,"ema50":0}
    c=df["close"]; factors=[]
    ema20=c.ewm(span=20).mean(); ema50=c.ewm(span=50).mean()
    factors.append("bullish" if ema20.iloc[-1]>ema20.iloc[-10] else "bearish")
    factors.append("bullish" if c.iloc[-1]>ema50.iloc[-1] else "bearish")
    h50=df["high"].tail(50); l50=df["low"].tail(50)
    if h50.iloc[-1]>h50.iloc[-25] and l50.iloc[-1]>l50.iloc[-25]: factors.append("bullish")
    elif h50.iloc[-1]<h50.iloc[-25] and l50.iloc[-1]<l50.iloc[-25]: factors.append("bearish")
    try:
        d=yf_fetch("NQ=F","1d","3mo")
        if not d.empty and len(d)>=20:
            dc=d["close"]
            factors.append("bullish" if dc.iloc[-1]>dc.ewm(span=20).mean().iloc[-1] else "bearish")
            factors.append("bullish" if dc.iloc[-1]>dc.iloc[-5] else "bearish")
    except: pass
    bull=factors.count("bullish"); bear=factors.count("bearish"); total=len(factors)
    b="bullish" if bull>bear else "bearish" if bear>bull else "neutral"
    s=max(bull,bear)/total if total else 0.5
    rh=df["high"].tail(100).max(); rl=df["low"].tail(100).min(); curr=c.iloc[-1]
    pos=(curr-rl)/(rh-rl) if rh!=rl else 0.5
    zone="premium" if pos>0.618 else "discount" if pos<0.382 else "equilibrium"
    return {"bias":b,"str":round(s,2),"zone":zone,"pos":round(pos,2),"ema20":float(ema20.iloc[-1]),"ema50":float(ema50.iloc[-1])}

def sweeps(df):
    lb=C["sweep_lb"]; out=[]
    if len(df)<lb+5: return out
    for i in range(lb,len(df)-1):
        w=df.iloc[i-lb:i]; ph=w["high"].max(); pl=w["low"].min()
        cur=df.iloc[i]; nxt=df.iloc[i+1]; age=len(df)-1-i
        if age>20: continue
        if cur["high"]>ph and nxt["close"]<ph and (cur["high"]-ph)>=C["sweep_min"]:
            out.append({"dir":"bsl","level":ph,"h":float(cur["high"]),"l":float(cur["low"]),"size":float(cur["high"]-ph),"age":age})
        elif cur["low"]<pl and nxt["close"]>pl and (pl-cur["low"])>=C["sweep_min"]:
            out.append({"dir":"ssl","level":pl,"h":float(cur["high"]),"l":float(cur["low"]),"size":float(pl-cur["low"]),"age":age})
    return sorted(out,key=lambda x:x["age"])

def fvgs(df):
    out=[]
    if len(df)<3: return out
    for i in range(1,len(df)-1):
        p,n=df.iloc[i-1],df.iloc[i+1]; age=len(df)-1-i
        if age>50: continue
        if p["high"]<n["low"] and (n["low"]-p["high"])>=C["fvg_min"]:
            out.append({"dir":"bullish","top":float(n["low"]),"bot":float(p["high"]),"mid":float((n["low"]+p["high"])/2),"size":float(n["low"]-p["high"]),"age":age})
        elif p["low"]>n["high"] and (p["low"]-n["high"])>=C["fvg_min"]:
            out.append({"dir":"bearish","top":float(p["low"]),"bot":float(n["high"]),"mid":float((p["low"]+n["high"])/2),"size":float(p["low"]-n["high"]),"age":age})
    return out

def smt(nq,es):
    if nq.empty or es.empty: return []
    lb=C["smt_lb"]; out=[]
    try: m=nq[["high","low","close"]].join(es[["high","low","close"]],lsuffix="_nq",rsuffix="_es",how="inner")
    except: return []
    if len(m)<lb: return []
    for i in range(lb,len(m)):
        w=m.iloc[i-lb:i]; c=m.iloc[i]
        if c["low_nq"]<w["low_nq"].min()*0.9995 and c["low_es"]>w["low_es"].min()*1.0005:
            mag=abs(c["low_nq"]-w["low_nq"].min())/w["low_nq"].min()*100
            if mag>0.02: out.append({"dir":"bullish","mag":round(mag,4),"confirmed":mag>0.06})
        elif c["high_nq"]>w["high_nq"].max()*1.0005 and c["high_es"]<w["high_es"].max()*0.9995:
            mag=abs(c["high_nq"]-w["high_nq"].max())/w["high_nq"].max()*100
            if mag>0.02: out.append({"dir":"bearish","mag":round(mag,4),"confirmed":mag>0.06})
    return out

def mss(df):
    if len(df)<20: return None
    r=df.tail(20); rh=r["high"].max(); rl=r["low"].min()
    hi=r["high"].idxmax(); li=r["low"].idxmin()
    c=df["close"].iloc[-1]; pc=df["close"].iloc[-2]
    if c>rh and pc<=rh and hi>li: return {"dir":"bullish","level":float(rh),"str":"strong" if c-rh>5 else "weak"}
    if c<rl and pc>=rl and li>hi: return {"dir":"bearish","level":float(rl),"str":"strong" if rl-c>5 else "weak"}
    return None

def pdz(df,lb=50):
    if len(df)<lb: return {"zone":"unknown","pos":0.5}
    rh=df["high"].tail(lb).max(); rl=df["low"].tail(lb).min(); curr=float(df["close"].iloc[-1])
    pos=(curr-rl)/(rh-rl) if rh!=rl else 0.5
    return {"zone":"premium" if pos>0.618 else "discount" if pos<0.382 else "equilibrium","pos":round(pos,2),"high":float(rh),"low":float(rl)}

def prev_levels():
    try:
        d=yf_fetch("NQ=F","1d","5d")
        if d.empty or len(d)<2: return {}
        p=d.iloc[-2]; return {"pdh":float(p["high"]),"pdl":float(p["low"]),"pdc":float(p["close"])}
    except: return {}

def overnight(df):
    try:
        now=datetime.now(ET); today=now.date()
        ny_open=ET.localize(datetime.combine(today,dtime(9,30)))
        prev=ET.localize(datetime.combine(today-timedelta(days=1),dtime(16,0)))
        mask=(df.index>=prev)&(df.index<ny_open); on=df[mask]
        if on.empty: return {}
        return {"onh":float(on["high"].max()),"onl":float(on["low"].min())}
    except: return {}

def kill_zone():
    t=datetime.now(ET).time()
    for n,kz in C["kill_zones"].items():
        s,e,w=kz["s"],kz["e"],kz["w"]
        if w==0: continue
        if (s<=t<e) if s<e else (t>=s or t<e): return n,w
    return None,0.0

def silver_bullet():
    t=datetime.now(ET).time()
    for sb in C["silver_bullets"]:
        if sb["s"]<=t<sb["e"]: return True,sb["n"]
    return False,None

# ──────────────────────────────────────────────────────────────
# NEWS
# ──────────────────────────────────────────────────────────────
@st.cache_data(ttl=180)
def news_sentiment():
    feeds=["https://feeds.reuters.com/reuters/businessNews","https://www.cnbc.com/id/100003114/device/rss/rss.html","https://finance.yahoo.com/news/rssindex"]
    bkw=["rate cut","stimulus","beat","trade deal","fed pivot","record high","soft landing"]
    rkw=["rate hike","inflation surges","recession","layoffs","trade war","tariff","crisis","miss"]
    tkw=["trump","white house","executive order","tariff announcement"]
    items=[]
    for url in feeds:
        try:
            feed=feedparser.parse(url)
            for e in feed.entries[:6]:
                t=e.get("title","")
                if not t: continue
                txt=(t+" "+e.get("summary","")[:200]).lower()
                vs=VADER.polarity_scores(txt)
                bull=sum(1 for k in bkw if k in txt); bear=sum(1 for k in rkw if k in txt)
                score=max(-1,min(1,vs["compound"]+(bull-bear)*0.1))
                hi=any(k in txt for k in ["fomc","cpi","nfp","gdp","fed decision"])
                items.append({"title":t[:85],"score":round(score,3),"dir":"🟢" if score>0.15 else "🔴" if score<-0.15 else "⚪","impact":"🔴 HIGH" if hi else "🟡 MED","trump":any(k in txt for k in tkw),"source":feed.feed.get("title","")[:15]})
        except: pass
    if not items: return {"score":0,"dir":"neutral","items":[],"pause":False}
    comp=sum(i["score"] for i in items)/len(items)
    pause=any(i["impact"]=="🔴 HIGH" and abs(i["score"])>0.5 for i in items)
    return {"score":round(comp,3),"dir":"bullish" if comp>0.15 else "bearish" if comp<-0.15 else "neutral","items":items[:8],"pause":pause}

# ──────────────────────────────────────────────────────────────
# CONFLUENCE SCORER
# ──────────────────────────────────────────────────────────────
def score(b,kz_n,kz_w,sw,sm,pz,sent,in_sb,ms,direction):
    total=0.0; bd={}
    # HTF
    if direction and b["bias"]!="neutral":
        aligned=(b["bias"]=="bullish" and direction=="long") or (b["bias"]=="bearish" and direction=="short")
        pts=C["sc"]["htf"]*b["str"] if aligned else 0
        total+=pts; bd["HTF Bias"]=f"+{pts:.1f} ✅ {b['bias'].upper()} ({b['str']:.0%})" if pts else f"❌ AGAINST ({b['bias']} vs {direction})"
    else: bd["HTF Bias"]="⚪ Neutral"
    # Kill Zone
    pts=C["sc"]["kz"]*kz_w if kz_n else 0; total+=pts
    bd["Kill Zone"]=f"+{pts:.1f} ✅ {kz_n}" if kz_n else "❌ No kill zone"
    # Silver Bullet
    if in_sb: total+=C["sc"]["sb_bonus"]; bd["Silver Bullet"]="⚡ +0.5 BONUS"
    # Sweep
    if sw:
        s=sw[0]; clean=s["size"]<(s["level"]*0.002); pts=C["sc"]["sweep"]*(1.0 if clean else 0.7)
        total+=pts; bd["Liquidity Sweep"]=f"+{pts:.1f} ✅ {s['dir'].upper()} @ {s['level']:.2f} ({s['size']:.1f}pt)"
    else: bd["Liquidity Sweep"]="❌ No sweep — setup incomplete"
    # SMT
    if sm:
        s=sm[-1]; pts=C["sc"]["smt"]*(1.0 if s["confirmed"] else 0.6); total+=pts
        bd["SMT NQ/ES"]=f"+{pts:.1f} ✅ {s['dir']} ({s['mag']:.4f}%{'  CONFIRMED' if s['confirmed'] else ''})"
    else: bd["SMT NQ/ES"]="❌ No divergence"
    # P/D
    if direction:
        ok=(direction=="long" and pz["zone"]=="discount") or (direction=="short" and pz["zone"]=="premium")
        pts=C["sc"]["pdz"] if ok else 0; total+=pts
        bd["P/D Zone"]=f"+{pts:.1f} ✅ {pz['zone'].upper()}" if pts else f"❌ {pz['zone'].upper()} (wrong for {direction})"
    # News
    if direction and sent:
        sd=sent.get("dir","neutral")
        ok=(direction=="long" and sd!="bearish") or (direction=="short" and sd!="bullish") or sd=="neutral"
        pts=C["sc"]["news"] if ok else 0; total+=pts
        bd["News"]=f"+{pts:.1f} ✅ {sd.upper()} ({sent['score']:+.3f})" if pts else f"❌ {sd.upper()} against"
        if sent.get("pause"): total=0; bd["⚠️ EVENT"]="SCORE ZEROED — high impact event"
    # MSS
    if ms: total+=C["sc"]["mss_bonus"]; bd["MSS"]=f"+0.5 ✅ {ms['dir'].upper()} break @ {ms['level']:.2f}"
    # Day weight
    dw=C["day_w"].get(datetime.now(ET).weekday(),0.7)
    if dw<0.7: total*=dw; bd["Day Weight"]=f"⚠️ {dw:.0%} weight day"
    else: bd["Day Weight"]=f"✅ {dw:.0%} conviction day"
    # VIX check
    try:
        vd=yf_fetch("^VIX","1d","5d")
        if not vd.empty:
            v=float(vd["close"].iloc[-1])
            if v>35: total=0; bd["VIX"]=f"🛑 {v:.1f} EXTREME"
            elif v>25: total*=0.75; bd["VIX"]=f"⚠️ {v:.1f} elevated"
            else: bd["VIX"]=f"✅ {v:.1f} normal"
    except: pass
    return round(min(total,10.0),2), bd

# ──────────────────────────────────────────────────────────────
# SIGNAL ENGINE
# ──────────────────────────────────────────────────────────────
def generate():
    nq=get_nq(); es=get_es()
    if nq.empty: return {"signal":False,"reason":"No data"}
    a=atr(nq); b=bias(nq); sw=sweeps(nq); fv=fvgs(nq)
    sm=smt(nq,es); ms=mss(nq); pz=pdz(nq)
    kz_n,kz_w=kill_zone(); in_sb,sb_n=silver_bullet()
    pl=prev_levels(); on=overnight(nq)
    try: sent=news_sentiment()
    except: sent={"score":0,"dir":"neutral","items":[],"pause":False}
    curr=float(nq["close"].iloc[-1]); curr_es=float(es["close"].iloc[-1]) if not es.empty else None
    direction="long" if (sw and sw[0]["dir"]=="ssl") else "short" if (sw and sw[0]["dir"]=="bsl") else None
    sc,bd=score(b,kz_n,kz_w,sw,sm,pz,sent,in_sb,ms,direction)
    base={"timestamp":datetime.now(ET).strftime("%H:%M:%S ET"),"price":curr,"es_price":curr_es,"atr":round(a,2),"bias":b,"sweeps":sw,"fvgs":fv,"smt":sm,"mss":ms,"pdz":pz,"pdl":pl,"overnight":on,"kz":kz_n,"kz_w":kz_w,"in_sb":in_sb,"sb_n":sb_n,"sent":sent,"score":sc,"bd":bd,"direction":direction,"signal":False}
    if sc<C["min_score"] or not direction:
        reason="No kill zone" if not kz_n else "No sweep" if not sw else f"Score {sc:.1f} < {C['min_score']}"
        return {**base,"reason":reason}
    sweep=sw[0]; rf=[f for f in fv if f["age"]<=20 and f["dir"]==("bullish" if direction=="long" else "bearish")]
    entry=rf[0]["mid"] if rf else (curr+a*0.05 if direction=="long" else curr-a*0.05)
    if direction=="long":
        stop=sweep["l"]-3; tp1=entry+abs(entry-stop)*C["target_rr"]; tp2=entry+abs(entry-stop)*(C["target_rr"]+1)
    else:
        stop=sweep["h"]+3; tp1=entry-abs(entry-stop)*C["target_rr"]; tp2=entry-abs(entry-stop)*(C["target_rr"]+1)
    rp=abs(entry-stop); rw=abs(tp1-entry); rr=rw/rp if rp>0 else 0
    if rr<C["min_rr"]: return {**base,"reason":f"R:R {rr:.2f} too low"}
    usd_risk=rp*C["tick_val"]; contracts=max(1,int(C["acct"]*C["risk_pct"]/usd_risk))
    sig={**base,"signal":True,"entry":round(entry,2),"stop":round(stop,2),"tp1":round(tp1,2),"tp2":round(tp2,2),"rp":round(rp,2),"rw":round(rw,2),"rr":round(rr,2),"usd_risk":round(usd_risk*contracts,2),"usd_reward":round(rw*C["tick_val"]*contracts,2),"contracts":contracts,"hc":sc>=C["high_conviction"],"has_fvg":bool(rf),"has_smt":bool(sm),"mss_conf":ms is not None}
    st.session_state.sigs.append({"t":sig["timestamp"],"dir":direction,"score":sc,"entry":entry,"stop":stop,"tp1":tp1,"rr":rr,"hc":sig["hc"],"kz":kz_n})
    if len(st.session_state.sigs)>100: st.session_state.sigs=st.session_state.sigs[-100:]
    return sig

# ──────────────────────────────────────────────────────────────
# CHART
# ──────────────────────────────────────────────────────────────
def chart(nq,sig=None,pl=None,on=None):
    df=nq.tail(120)
    fig=make_subplots(rows=3,cols=1,shared_xaxes=True,row_heights=[0.65,0.2,0.15],vertical_spacing=0.015)
    fig.add_trace(go.Candlestick(x=df.index,open=df["open"],high=df["high"],low=df["low"],close=df["close"],increasing=dict(line_color="#22c55e",fillcolor="#22c55e"),decreasing=dict(line_color="#ef4444",fillcolor="#ef4444"),name="NQ"),row=1,col=1)
    tp=(df["high"]+df["low"]+df["close"])/3
    vwap=(tp*df["volume"]).cumsum()/df["volume"].cumsum()
    fig.add_trace(go.Scatter(x=df.index,y=vwap,line=dict(color="#a78bfa",width=1.5,dash="dot"),name="VWAP"),row=1,col=1)
    e20=df["close"].ewm(span=20).mean(); e50=df["close"].ewm(span=50).mean()
    fig.add_trace(go.Scatter(x=df.index,y=e20,line=dict(color="#f59e0b",width=1),name="EMA20",opacity=0.7),row=1,col=1)
    fig.add_trace(go.Scatter(x=df.index,y=e50,line=dict(color="#60a5fa",width=1),name="EMA50",opacity=0.7),row=1,col=1)
    if pl:
        for n,c,v in [("PDH","#f59e0b",pl.get("pdh")),("PDL","#ef4444",pl.get("pdl")),("PDC","#6b7280",pl.get("pdc"))]:
            if v: fig.add_hline(y=v,line=dict(color=c,width=1,dash="dash"),annotation_text=f"{n}:{v:.0f}",annotation_font=dict(size=9,color=c),row=1,col=1)
    if on:
        for n,c,v in [("ONH","#06b6d4",on.get("onh")),("ONL","#ec4899",on.get("onl"))]:
            if v: fig.add_hline(y=v,line=dict(color=c,width=1,dash="dot"),annotation_text=f"{n}:{v:.0f}",annotation_font=dict(size=9,color=c),row=1,col=1)
    if sig and sig.get("signal"):
        col="#22c55e" if sig["direction"]=="long" else "#ef4444"
        for n,c,v in [("ENTRY",col,sig["entry"]),("STOP","#ef4444",sig["stop"]),("TP1","#22c55e",sig["tp1"]),("TP2","#60a5fa",sig["tp2"])]:
            fig.add_hline(y=v,line=dict(color=c,width=2),annotation_text=f"◄{n} {v:.2f}",annotation_font=dict(size=10,color=c,family="Courier New"),row=1,col=1)
    vc=["#22c55e" if c>=o else "#ef4444" for c,o in zip(df["close"],df["open"])]
    fig.add_trace(go.Bar(x=df.index,y=df["volume"],marker_color=vc,opacity=0.6),row=2,col=1)
    delta=df["close"].diff(); g=delta.where(delta>0,0).rolling(14).mean(); lo=(-delta.where(delta<0,0)).rolling(14).mean()
    rsi=100-(100/(1+g/lo))
    fig.add_trace(go.Scatter(x=df.index,y=rsi,line=dict(color="#a78bfa",width=1.5)),row=3,col=1)
    for y,c in [(70,"#ef4444"),(50,"#374151"),(30,"#22c55e")]: fig.add_hline(y=y,line=dict(color=c,width=1,dash="dot"),row=3,col=1)
    fig.update_layout(template="plotly_dark",height=580,showlegend=False,margin=dict(l=50,r=40,t=15,b=10),plot_bgcolor="#05080f",paper_bgcolor="#05080f",xaxis_rangeslider_visible=False)
    return fig

# ──────────────────────────────────────────────────────────────
# BACKTEST
# ──────────────────────────────────────────────────────────────
def backtest(period="6mo"):
    nq=yf_fetch("NQ=F","5m",period); es=yf_fetch("ES=F","5m",period)
    if nq.empty: return {"error":"No data"}
    trades=[]; daily={}; in_t=None; cl=0; lb=30
    for i in range(lb,len(nq)-2):
        bt=nq.index[i]; t=bt.time()
        in_kz=any((kz["s"]<=t<kz["e"] if kz["s"]<kz["e"] else (t>=kz["s"] or t<kz["e"])) for kz in C["kill_zones"].values() if kz["w"]>0)
        if not in_kz: continue
        dw=C["day_w"].get(bt.weekday(),0)
        if dw<0.5: continue
        dk=bt.date()
        if daily.get(dk,0)>=C["max_day"]: continue
        if cl>=3: continue
        bar=nq.iloc[i]
        if in_t:
            if in_t["d"]=="long":
                if bar["low"]<=in_t["stop"]: trades.append({**in_t,"pnl":in_t["stop"]-in_t["e"],"r":"loss"}); cl+=1; in_t=None
                elif bar["high"]>=in_t["tp1"]: trades.append({**in_t,"pnl":in_t["tp1"]-in_t["e"],"r":"win"}); cl=0; in_t=None
            else:
                if bar["high"]>=in_t["stop"]: trades.append({**in_t,"pnl":in_t["e"]-in_t["stop"],"r":"loss"}); cl+=1; in_t=None
                elif bar["low"]<=in_t["tp1"]: trades.append({**in_t,"pnl":in_t["e"]-in_t["tp1"],"r":"win"}); cl=0; in_t=None
            continue
        w=nq.iloc[max(0,i-lb):i+1]; ew=es.iloc[max(0,i-lb):i+1] if not es.empty else pd.DataFrame()
        sw=sweeps(w); sm=smt(w,ew); fv=fvgs(w)
        if not sw: continue
        sweep=sw[0]; d="long" if sweep["dir"]=="ssl" else "short"
        sc=5.0
        if sm: sc+=2.0
        if fv: sc+=1.0
        if dw==1.0: sc+=0.5
        if sc<C["min_score"]: continue
        a_val=atr(w); curr=float(w["close"].iloc[-1])
        rf=[f for f in fv if f["dir"]==("bullish" if d=="long" else "bearish") and f["age"]<=15]
        entry=rf[0]["mid"] if rf else curr
        if d=="long": stop=sweep["l"]-3; tp1=entry+abs(entry-stop)*C["target_rr"]
        else: stop=sweep["h"]+3; tp1=entry-abs(entry-stop)*C["target_rr"]
        rp=abs(entry-stop); rr=abs(tp1-entry)/rp if rp>0 else 0
        if rp<5 or rr<C["min_rr"]: continue
        in_t={"d":d,"e":entry,"stop":stop,"tp1":tp1,"sc":sc,"bt":bt}
        daily[dk]=daily.get(dk,0)+1
    if not trades: return {"error":"No trades in this period","period":period}
    wins=[t for t in trades if t["r"]=="win"]; losses=[t for t in trades if t["r"]=="loss"]
    wr=len(wins)/len(trades); aw=np.mean([t["pnl"] for t in wins]) if wins else 0
    al=abs(np.mean([t["pnl"] for t in losses])) if losses else 0.001
    gp=sum(t["pnl"] for t in wins); gl=abs(sum(t["pnl"] for t in losses))
    pf=gp/(gl+0.001); tp=sum(t["pnl"] for t in trades); tu=tp*C["tick_val"]
    eq=[C["acct"]]+[0]*(len(trades))
    for i,t in enumerate(trades): eq[i+1]=eq[i]+t["pnl"]*C["tick_val"]
    peak=eq[0]; mdd=0
    for e in eq:
        if e>peak: peak=e
        dd=(peak-e)/peak if peak>0 else 0; mdd=max(mdd,dd)
    sh=(np.mean([t["pnl"] for t in trades])/(np.std([t["pnl"] for t in trades])+0.001))*(252**0.5)
    by_day={}
    for t in trades:
        dn=t["bt"].strftime("%A")
        if dn not in by_day: by_day[dn]={"t":0,"w":0,"pnl":0}
        by_day[dn]["t"]+=1; by_day[dn]["pnl"]+=t["pnl"]*C["tick_val"]
        if t["r"]=="win": by_day[dn]["w"]+=1
    return {"total":len(trades),"wins":len(wins),"losses":len(losses),"wr":wr,"aw":aw,"al":al,"pf":pf,"tp":tp,"tu":tu,"mdd":mdd,"sh":sh,"eq":eq,"by_day":by_day,"period":period}

# ──────────────────────────────────────────────────────────────
# SIDEBAR
# ──────────────────────────────────────────────────────────────
with st.sidebar:
    st.markdown("## ⚡ PB Intelligence")
    st.markdown(f"*v3.0 · Ultimate Weapon*")
    if st.session_state.tv_ok:
        st.markdown('<span style="color:#22c55e;font-size:11px">● LIVE — Tradovate</span>', unsafe_allow_html=True)
    else:
        st.markdown(f'<span style="color:#f59e0b;font-size:11px">● {data_src()}</span>', unsafe_allow_html=True)
    st.markdown("---")
    with st.expander("🔗 Connect Tradovate", expanded=not st.session_state.tv_ok):
        st.caption("Free demo account at tradovate.com")
        tv_u=st.text_input("Username",key="tvu")
        tv_p=st.text_input("Password",type="password",key="tvp")
        tv_l=st.checkbox("Live account",False)
        if st.button("Connect",type="primary",use_container_width=True):
            with st.spinner("Connecting..."):
                ok,msg=tv_connect(tv_u,tv_p,tv_l)
            st.success(msg) if ok else st.error(msg)
    st.markdown("---")
    for lbl,pg in [("📡 Live Signals","signals"),("📊 Backtest","backtest"),("📰 News","news"),("📈 Log","log"),("⚙️ Setup","setup")]:
        if st.button(lbl,use_container_width=True,type="primary" if st.session_state.page==pg else "secondary"):
            st.session_state.page=pg
    st.markdown("---")
    et_now=datetime.now(ET); kz_n,kz_w=kill_zone(); in_sb,sb_n=silver_bullet()
    dw=C["day_w"].get(et_now.weekday(),0)
    st.markdown(f"**{et_now.strftime('%H:%M:%S ET')}**")
    st.markdown(f"*{et_now.strftime('%A %b %d')}*")
    kz_e="🟢" if kz_n and kz_w==1.0 else "🟡" if kz_n else "⭕"
    st.markdown(f"{kz_e} {kz_n or 'No Kill Zone'}")
    if in_sb: st.markdown(f"⚡ **{sb_n}**")
    st.markdown(f"Day weight: **{dw:.0%}**")
    st.markdown(f"Scans: **{st.session_state.scans}**")
    st.markdown("---")
    auto=st.checkbox("Auto-scan (30s)",True)
    if st.button("🔄 Scan Now",type="primary",use_container_width=True):
        st.cache_data.clear(); st.rerun()

# ──────────────────────────────────────────────────────────────
# PAGES
# ──────────────────────────────────────────────────────────────
pg=st.session_state.page

if pg=="signals":
    sig=generate(); st.session_state.scans+=1
    nq_p=live_price("NQ") or sig.get("price",0); es_p=live_price("ES") or sig.get("es_price",0)
    c1,c2,c3,c4,c5=st.columns([2,1,1,1,1])
    with c1:
        st.markdown(f'<div class="pricebig">{nq_p:,.2f}</div>', unsafe_allow_html=True)
        st.caption(f"NQ Futures — {data_src()}")
    with c2: st.metric("ES Futures", f"{es_p:,.2f}" if es_p else "—")
    with c3: st.metric("Score", f"{sig['score']:.1f}/10", "🔥 SIGNAL" if sig.get("signal") else "Scanning…")
    with c4: st.metric("Kill Zone", (sig.get("kz") or "None")[:12], "ACTIVE" if sig.get("kz") else "Waiting")
    with c5: st.metric("Silver Bullet", "⚡ YES" if sig.get("in_sb") else "No")
    st.markdown("---")
    if sig.get("signal"):
        d=sig["direction"]; col="#22c55e" if d=="long" else "#ef4444"; dlbl="▲  LONG" if d=="long" else "▼  SHORT"
        banner="🔥 HIGH CONVICTION " if sig["hc"] else "⚡ SILVER BULLET " if sig["in_sb"] else "✅ "
        (st.success if sig["hc"] or sig["in_sb"] else st.info)(f"{banner}{dlbl} — Score {sig['score']:.1f}/10")
        a,b_,c_,d_=st.columns(4)
        with a: st.markdown(f'<div class="card"><div class="ct">Direction</div><div style="font-size:28px;font-weight:bold;color:{col}">{dlbl}</div><div style="font-size:11px;color:#3a5070;margin-top:6px">Zone: {sig.get("kz","?")} {"⚡SB" if sig["in_sb"] else ""}</div></div>',unsafe_allow_html=True)
        with b_: st.markdown(f'<div class="card"><div class="ct">Entry</div><div style="font-size:24px;font-weight:bold;color:#e8f0ff;font-family:monospace">{sig["entry"]:,.2f}</div><div style="font-size:11px;color:#3a5070;margin-top:6px">{"IFVG midpoint" if sig["has_fvg"] else "Market"}</div></div>',unsafe_allow_html=True)
        with c_: st.markdown(f'<div class="card"><div class="ct">Stop Loss</div><div style="font-size:24px;font-weight:bold;color:#ef4444;font-family:monospace">{sig["stop"]:,.2f}</div><div style="font-size:11px;color:#3a5070;margin-top:6px">Risk: {sig["rp"]:.0f} pts / ${sig["usd_risk"]:,.0f}</div></div>',unsafe_allow_html=True)
        with d_: st.markdown(f'<div class="card"><div class="ct">Targets</div><div style="font-size:18px;font-weight:bold;color:#22c55e;font-family:monospace">TP1: {sig["tp1"]:,.2f}</div><div style="font-size:18px;font-weight:bold;color:#60a5fa;font-family:monospace">TP2: {sig["tp2"]:,.2f}</div><div style="font-size:11px;color:#3a5070;margin-top:4px">1:{sig["rr"]:.1f} R:R — ${sig["usd_reward"]:,.0f} at TP1</div></div>',unsafe_allow_html=True)
        e1,e2=st.columns(2)
        with e1:
            with st.expander("📊 Confluence Breakdown",expanded=True):
                for k,v in sig.get("bd",{}).items(): st.markdown(f"**{k}:** {v}")
        with e2:
            with st.expander("🔍 Components",expanded=True):
                sw2=sig.get("sweeps",[]); sm2=sig.get("smt",[]); ms2=sig.get("mss")
                for ok,lbl in [(bool(sw2),f"Sweep: {sw2[0]['dir'].upper()} @ {sw2[0]['level']:.2f}" if sw2 else "No sweep"),(bool(sm2),f"SMT: {sm2[-1]['dir']} ({sm2[-1]['mag']:.4f}%)" if sm2 else "No SMT"),(sig["has_fvg"],f"IFVG entry @ {sig['entry']:.2f}"),(ms2 is not None,f"MSS: {ms2['dir'].upper()} @ {ms2['level']:.2f}" if ms2 else "No MSS"),(bool(sig.get("kz")),f"Kill Zone: {sig.get('kz','')}"),(sig["in_sb"],f"Silver Bullet"),(sig["bias"]["bias"]!="neutral",f"HTF: {sig['bias']['bias'].upper()} {sig['bias']['str']:.0%}"),(sig["pdz"]["zone"]!="equilibrium",f"P/D: {sig['pdz']['zone'].upper()} {sig['pdz']['pos']:.0%}")]:
                    st.markdown(f'<span style="color:{"#22c55e" if ok else "#4a6080"}">{"✅" if ok else "❌"} {lbl}</span>',unsafe_allow_html=True)
    else:
        sc=sig.get("score",0); pct=int((sc/10)*100)
        bc="green" if sc>=8.5 else "yellow" if sc>=7 else "red"
        st.markdown(f'<div class="sig-wait"><div style="font-size:16px;color:#2a5080;margin-bottom:10px">◈ SCANNING — {sig.get("reason","Monitoring market...")}</div><div style="display:flex;justify-content:space-between;margin-bottom:4px"><span style="color:#3a5070;font-size:11px">SCORE</span><span style="font-weight:bold;color:{"#22c55e" if sc>=8.5 else "#f59e0b" if sc>=7 else "#4a6080"}">{sc:.1f}/10</span></div><div style="height:8px;background:#0a1020;border-radius:4px"><div style="height:100%;width:{pct}%;background:{"#22c55e" if bc=="green" else "#f59e0b" if bc=="yellow" else "#ef4444"};border-radius:4px"></div></div></div>',unsafe_allow_html=True)
        wa1,wa2=st.columns(2)
        with wa1:
            st.markdown("**Market State**")
            b2=sig.get("bias",{"bias":"?","str":0,"zone":"?"})
            pz2=sig.get("pdz",{"zone":"?","pos":0})
            be="🟢" if b2["bias"]=="bullish" else "🔴" if b2["bias"]=="bearish" else "⚪"
            ze="🟢" if pz2["zone"]=="discount" else "🔴" if pz2["zone"]=="premium" else "⚪"
            sw2=sig.get("sweeps",[]); sm2=sig.get("smt",[])
            st.markdown(f"{be} HTF: **{b2['bias'].upper()}** ({b2.get('str',0):.0%})\n{ze} Zone: **{pz2['zone'].upper()}**\n{'🟢' if sw2 else '❌'} Sweeps: **{len(sw2)}**\n{'🟢' if sm2 else '❌'} SMT: **{len(sm2)}**")
        with wa2:
            st.markdown("**Best Windows (ET)**")
            st.markdown("• 9:30–11:00 AM 🔥 Primary\n• 10:00–11:00 ⚡ Silver Bullet\n• 11:30–1 PM ❌ AVOID\n• 2:00–3:00 PM ⚡ PM Silver Bullet\n• **Tue > Wed > Thu**")
    st.markdown("---")
    nq=get_nq()
    if not nq.empty:
        pl2=sig.get("pdl") or prev_levels(); on2=sig.get("overnight") or overnight(nq)
        st.plotly_chart(chart(nq,sig if sig.get("signal") else None,pl2,on2),use_container_width=True)
    st.markdown("---")
    ctx1,ctx2,ctx3=st.columns(3)
    with ctx1:
        st.markdown("**📐 Key Levels**")
        pl2=sig.get("pdl",{}); on2=sig.get("overnight",{}); b2=sig.get("bias",{})
        for lbl,val in [("PDH",pl2.get("pdh")),("PDL",pl2.get("pdl")),("PDC",pl2.get("pdc")),("ONH",on2.get("onh")),("ONL",on2.get("onl")),("EMA20",b2.get("ema20")),("EMA50",b2.get("ema50"))]:
            if val: st.markdown(f'<div class="row"><span class="lv">{lbl}</span><span class="rv">{val:,.2f}</span></div>',unsafe_allow_html=True)
    with ctx2:
        st.markdown("**🌐 Internals**")
        try:
            vd=yf_fetch("^VIX","1d","5d"); hd=yf_fetch("HYG","1d","5d")
            if not vd.empty:
                v=float(vd["close"].iloc[-1]); vc="🔴" if v>25 else "🟢"
                st.markdown(f'<div class="row"><span class="lv">VIX</span><span class="rv" style="color:{"#ef4444" if v>25 else "#22c55e"}">{v:.2f}</span></div>',unsafe_allow_html=True)
            if not hd.empty and len(hd)>1:
                h=float(hd["close"].iloc[-1]); hc=((h/float(hd["close"].iloc[-2]))-1)*100
                st.markdown(f'<div class="row"><span class="lv">HYG Credit</span><span class="rv" style="color:{"#22c55e" if hc>=0 else "#ef4444"}">{h:.2f} ({hc:+.2f}%)</span></div>',unsafe_allow_html=True)
        except: st.caption("Loading...")
    with ctx3:
        st.markdown("**📰 Sentiment**")
        try:
            ns=news_sentiment(); se="🟢" if ns["dir"]=="bullish" else "🔴" if ns["dir"]=="bearish" else "⚪"
            st.markdown(f'{se} **{ns["dir"].upper()}** ({ns["score"]:+.3f})')
            if ns.get("pause"): st.error("⚠️ HIGH IMPACT EVENT")
            for h in ns.get("items",[])[:3]: st.markdown(f'{h["dir"]} {h["title"][:60]}')
        except: st.caption("Loading news...")
    if auto: time.sleep(1); st.rerun()

elif pg=="backtest":
    st.markdown("# 📊 Backtest — Real Historical NQ Data")
    l,r=st.columns([1,3])
    with l:
        period=st.selectbox("Period",["3mo","6mo","1y"],index=1)
        if st.button("▶ Run Backtest",type="primary",use_container_width=True):
            with st.spinner(f"Running on real NQ {period} data..."):
                st.session_state.bt=backtest(period)
    with r:
        res=st.session_state.bt
        if res and "error" not in res:
            m1,m2,m3,m4,m5=st.columns(5)
            m1.metric("Trades",res["total"],f"{res['wins']}W/{res['losses']}L")
            m2.metric("Win Rate",f"{res['wr']:.1%}")
            m3.metric("Profit Factor",f"{res['pf']:.2f}")
            m4.metric("Net P&L",f"${res['tu']:+,.0f}")
            m5.metric("Max DD",f"{res['mdd']:.1%}")
            m1.metric("Avg Win",f"{res['aw']:.1f}pt (${res['aw']*C['tick_val']:.0f})")
            m2.metric("Avg Loss",f"{res['al']:.1f}pt (${res['al']*C['tick_val']:.0f})")
            m3.metric("Sharpe",f"{res['sh']:.2f}")
            if res.get("eq"):
                fe=go.Figure(go.Scatter(y=res["eq"],fill="tozeroy",fillcolor="rgba(34,197,94,0.08)",line=dict(color="#22c55e",width=2)))
                fe.add_hline(y=C["acct"],line=dict(color="#374151",dash="dot"),annotation_text="Starting Capital")
                fe.update_layout(title="Equity Curve",template="plotly_dark",height=220,plot_bgcolor="#05080f",paper_bgcolor="#05080f",margin=dict(l=40,r=20,t=30,b=20))
                st.plotly_chart(fe,use_container_width=True)
            if res.get("by_day"):
                rows=[{"Day":d,"Trades":v["t"],"Win Rate":f"{v['w']/v['t']:.0%}" if v["t"] else "—","P&L":f"${v['pnl']:+,.0f}"} for d,v in res["by_day"].items()]
                st.dataframe(pd.DataFrame(rows),use_container_width=True,hide_index=True)
        elif res and "error" in res: st.error(res["error"])
        else: st.info("Click Run Backtest to see real performance on historical NQ data")

elif pg=="news":
    st.markdown("# 📰 News Intelligence")
    if st.button("🔄 Refresh"): st.cache_data.clear()
    with st.spinner("Loading..."):
        ns=news_sentiment()
    n1,n2,n3=st.columns(3)
    se="🟢" if ns["dir"]=="bullish" else "🔴" if ns["dir"]=="bearish" else "⚪"
    n1.metric("Sentiment",f"{se} {ns['dir'].upper()}",f"{ns['score']:+.3f}")
    n2.metric("Headlines",len(ns.get("items",[])))
    n3.metric("Pause Trading?","⚠️ YES" if ns.get("pause") else "✅ No")
    if ns.get("pause"): st.error("⚠️ HIGH IMPACT EVENT — Avoid trading now")
    if ns.get("items"): st.dataframe(pd.DataFrame(ns["items"]).rename(columns={"dir":"📊","impact":"Impact","title":"Headline","source":"Source","score":"Score","trump":"Trump"}),use_container_width=True,height=400)

elif pg=="log":
    st.markdown("# 📈 Signal Log")
    if st.session_state.sigs:
        st.dataframe(pd.DataFrame(st.session_state.sigs),use_container_width=True,height=400)
    else: st.info("Signals will appear here as the system scans.")
    st.markdown("---")
    st.markdown("### Log a Manual Trade")
    c1,c2=st.columns(2)
    with c1:
        td=st.selectbox("Direction",["long","short"]); te=st.number_input("Entry",value=20000.0); tx=st.number_input("Exit",value=20050.0)
    with c2:
        tr=st.selectbox("Result",["win","loss"]); ts=st.number_input("Score",0.0,10.0,7.0); tn=st.text_area("Notes")
    if st.button("Log Trade"):
        pnl=tx-te if td=="long" else te-tx
        pd.DataFrame([{"time":datetime.now(ET).strftime("%H:%M:%S"),"dir":td,"entry":te,"exit":tx,"result":tr,"pnl_pts":round(pnl,2),"pnl_usd":round(pnl*C["tick_val"],2),"score":ts,"notes":tn}]).to_csv("trades.csv",mode="a",header=not os.path.exists("trades.csv"),index=False)
        st.success("✅ Logged")

elif pg=="setup":
    st.markdown("# ⚙️ Launch Guide")
    st.code("""# Install (one time)
pip install -r requirements.txt

# Run
streamlit run app.py
# Opens at http://localhost:8501""","bash")
    st.markdown("---")
    st.markdown("### Deploy Free to the Web")
    st.code("""git init && git add . && git commit -m "PB v3"
git remote add origin https://github.com/YOURNAME/pb-intelligence.git
git push -u origin main
# Then: share.streamlit.io → connect repo → Deploy
# Live at: https://pb-intelligence.streamlit.app""","bash")
    st.markdown("---")
    st.markdown("### Tradovate (Real-Time Futures Data)")
    st.markdown("1. Sign up free at **tradovate.com** (demo = no cost)\n2. Enter credentials in the sidebar\n3. Click Connect — live NQ/ES data streams immediately\n4. For live trading: fund your account, switch to Live in sidebar")
    st.markdown("---")
    st.markdown("### How Signals Work")
    st.markdown("""
| Step | Action |
|------|--------|
| System fires signal | Entry, Stop, TP1, TP2 displayed |
| You open trade | Enter at shown price in Tradovate |
| Set stop loss | Exactly as shown |
| Set limit at TP1 | Let it run |
| Move stop to breakeven | Once TP1 hit |
| Let TP2 run | Or close manually |

**Min score 7.0 to fire. High conviction = 8.5+.**
""")

st.markdown("---")
sc=st.session_state.scans; ls=st.session_state.get("last_scan")
st.markdown(f"<div style='text-align:center;color:#1a2840;font-size:11px;font-family:monospace'>PB Intelligence v3.0 · Scans: {sc} · {data_src()} · Trade at your own risk</div>",unsafe_allow_html=True)
