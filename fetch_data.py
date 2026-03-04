"""
Fetches Apex Algos P&L data from Google Sheets and writes data.json.
Runs as a GitHub Action — no API key needed, sheet must be public.
UPDATED: Proper date parsing, DD/MM/YY format, and data ordering
"""
import requests, json, csv, io, datetime, statistics, sys

SHEET_ID  = '1Rgy-HH8bcY7guN7PuH6biiK-JVV-PaFOH5aDII8oVf8'
CAP_NAP   = 250_000
CAP_SAP   = 250_000

def fetch_csv(sheet_name):
    url = (
        f'https://docs.google.com/spreadsheets/d/{SHEET_ID}'
        f'/gviz/tq?tqx=out:csv&sheet={requests.utils.quote(sheet_name)}'
    )
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    return r.text

def parse_monthly(raw_csv):
    """Parse Monthly P&L sheet — returns list of {date, nap, sap} sorted oldest first."""
    reader = csv.reader(io.StringIO(raw_csv))
    rows   = list(reader)
    result = []
    for row in rows:
        if not row or not row[0].strip():
            continue
        try:
            d = datetime.datetime.strptime(row[0].strip(), '%m/%d/%Y')
        except ValueError:
            try:
                d = datetime.datetime.fromisoformat(row[0].strip())
            except Exception:
                continue
        try:
            nap = float(row[1].replace(',', '').replace('"','').strip()) if row[1].strip() else 0
            sap = float(row[3].replace(',', '').replace('"','').strip()) if len(row) > 3 and row[3].strip() else 0
        except (ValueError, IndexError):
            continue
        result.append({'date': d.strftime('%Y-%m'), 'nap': nap, 'sap': sap, 'date_obj': d})
    result.sort(key=lambda x: x['date'])
    return result

def parse_daily(raw_csv):
    """Parse DAILY P&L sheet — returns (dates, nap_list, sap_list) sorted oldest first."""
    reader = csv.reader(io.StringIO(raw_csv))
    rows   = list(reader)
    data = []
    
    for row in rows:
        if not row or not row[0].strip():
            continue
        try:
            d = datetime.datetime.strptime(row[0].strip(), '%m/%d/%Y')
        except ValueError:
            continue
        try:
            nap = float(row[1].replace(',','').replace('"','').strip()) if row[1].strip() else None
            sap = float(row[4].replace(',','').replace('"','').strip()) if len(row) > 4 and row[4].strip() else None
        except (ValueError, IndexError):
            continue
        
        # Store date along with values
        data.append({
            'date': d,
            'nap': nap,
            'sap': sap
        })
    
    # Sort by date (oldest first)
    data.sort(key=lambda x: x['date'])
    
    # Extract into separate lists with DD/MM/YY format
    dates = [d['date'].strftime('%d/%m/%y') for d in data]
    nap_list = [d['nap'] if d['nap'] is not None else 0 for d in data]
    sap_list = [d['sap'] if d['sap'] is not None else 0 for d in data]
    
    return dates, nap_list, sap_list

def sharpe(daily, capital):
    if len(daily) < 2:
        return 0
    r = [d / capital for d in daily]
    return round((statistics.mean(r) / statistics.stdev(r)) * (252 ** 0.5), 2)

def max_drawdown(daily, capital):
    cum = peak = 0
    dd  = 0
    for d in daily:
        cum  += d
        peak  = max(peak, cum)
        dd    = min(dd, (cum - peak) / capital)
    return round(dd * 100, 1)

def win_rate(daily):
    if not daily:
        return 0
    return round(sum(1 for d in daily if d > 0) / len(daily) * 100, 1)

def build_data():
    print("Fetching Monthly P&L...")
    monthly_csv = fetch_csv('Monthly P&L')
    monthly     = parse_monthly(monthly_csv)

    print("Fetching Daily P&L...")
    daily_csv = fetch_csv('DAILY P&L')
    daily_dates, nap_daily, sap_daily = parse_daily(daily_csv)

    months = []
    nap_m = []
    sap_m = []
    for row in monthly:
        # Format as "Mon YY" for display
        d = row['date_obj']
        months.append(d.strftime('%b %y'))
        nap_m.append(row['nap'])
        sap_m.append(row['sap'])

    comb_daily = [n + s for n, s in zip(nap_daily, sap_daily)]

    # Get today's date in DD/MM/YY format
    today_formatted = datetime.date.today().strftime('%d/%m/%y')

    data = {
        'updated':     today_formatted,  # Changed to DD/MM/YY
        'months':      months,
        'nap_monthly': nap_m,
        'sap_monthly': sap_m,
        'nap_daily':   nap_daily,
        'sap_daily':   sap_daily,
        'daily_dates': daily_dates,
        'stats': {
            'nap': {
                'roi':      round(sum(nap_m) / CAP_NAP * 100, 1),
                'pnl':      round(sum(nap_m)),
                'sharpe':   sharpe(nap_daily, CAP_NAP),
                'max_dd':   max_drawdown(nap_daily, CAP_NAP),
                'win_rate': win_rate(nap_daily),
                'days':     len(nap_daily),
            },
            'sap': {
                'roi':      round(sum(sap_m) / CAP_SAP * 100, 1),
                'pnl':      round(sum(sap_m)),
                'sharpe':   sharpe(sap_daily, CAP_SAP),
                'max_dd':   max_drawdown(sap_daily, CAP_SAP),
                'win_rate': win_rate(sap_daily),
                'days':     len(sap_daily),
            },
            'comb': {
                'roi':      round((sum(nap_m) + sum(sap_m)) / (CAP_NAP + CAP_SAP) * 100, 1),
                'pnl':      round(sum(nap_m) + sum(sap_m)),
                'sharpe':   sharpe(comb_daily, CAP_NAP + CAP_SAP),
                'max_dd':   max_drawdown(comb_daily, CAP_NAP + CAP_SAP),
                'win_rate': win_rate(comb_daily),
            },
        },
    }
    
    print(f"✓ Parsed {len(nap_daily)} daily records")
    print(f"✓ Date range: {daily_dates[0]} to {daily_dates[-1]}")
    print(f"✓ Updated date: {today_formatted}")
    
    return data

if __name__ == '__main__':
    try:
        data = build_data()
        with open('data.json', 'w') as f:
            json.dump(data, f, indent=2)
        print(f"✓ data.json written — {len(data['months'])} months, updated {data['updated']}")
    except Exception as e:
        print(f"✗ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
