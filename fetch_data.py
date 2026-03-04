"""
Fetches Apex Algos P&L data from Google Sheets and writes data.json.
CUSTOM VERSION for your specific data format (DD-MM-YY dates, ₹ symbols)
"""
import requests, json, csv, io, datetime, statistics, sys
import re

SHEET_ID  = '1Rgy-HH8bcY7guN7PuH6biiK-JVV-PaFOH5aDII8oVf8'
CAP_NAP   = 250_000
CAP_SAP   = 250_000
CAP_CAP   = 500_000
CAP_NAPV2 = 250_000
CAP_NAPV3 = 250_000
CAP_SAPV2 = 250_000
CAP_SAPV3 = 250_000

# Column indices (0-based): A=0, B=1 ... L=11, S=18, V=21, Y=24, AB=27
COL_NAP   = 1
COL_SAP   = 4
COL_CAP   = 11
COL_NAPV2 = 18
COL_NAPV3 = 21
COL_SAPV2 = 24
COL_SAPV3 = 27

def clean_number(value):
    """Remove ₹, commas, quotes and convert to float"""
    if not value or not value.strip():
        return None
    # Remove ₹ symbol, commas, quotes, and extra spaces
    cleaned = value.replace('₹', '').replace(',', '').replace('"', '').strip()
    try:
        return float(cleaned)
    except ValueError:
        return None

def parse_date_ddmmyy(date_str):
    """Parse date in DD-MM-YY (Day of Week) format"""
    # Format: "02-03-26 (Mon)" or "02-03-26"
    # Extract just the date part
    date_part = date_str.split('(')[0].strip()
    try:
        # Parse DD-MM-YY format
        d = datetime.datetime.strptime(date_part, '%d-%m-%y')
        return d
    except ValueError:
        return None

def fetch_csv(sheet_name):
    url = (
        f'https://docs.google.com/spreadsheets/d/{SHEET_ID}'
        f'/gviz/tq?tqx=out:csv&sheet={requests.utils.quote(sheet_name)}'
    )
    print(f"  Fetching: {url[:80]}...")
    r = requests.get(url, timeout=15)
    r.raise_for_status()
    print(f"  ✓ Response received: {len(r.text)} bytes")
    return r.text

def parse_daily_pnl(raw_csv):
    """Parse DAILY P&L sheet with DD-MM-YY format"""
    reader = csv.reader(io.StringIO(raw_csv))
    rows = list(reader)
    data = []
    
    print(f"  Found {len(rows)} total rows")
    
    # Debug: print first 10 rows to understand structure
    print("  DEBUG — first 10 rows:")
    for i, row in enumerate(rows[:10]):
        print(f"    Row {i}: {row[:5]}")  # print first 5 cols only
    
    # Try to auto-detect the first data row by finding where dates start
    start_row = 0
    for i, row in enumerate(rows):
        if row and row[0].strip():
            parsed = parse_date_ddmmyy(row[0])
            if parsed:
                start_row = i
                print(f"  Auto-detected data start at row {i}: '{row[0]}'")
                break

    for i, row in enumerate(rows[start_row:], start=start_row):
        if not row or not row[0].strip():
            continue
        
        date = parse_date_ddmmyy(row[0])
        if not date:
            continue

        def get_col(col_idx):
            return clean_number(row[col_idx]) if len(row) > col_idx else None

        nap   = get_col(COL_NAP)
        sap   = get_col(COL_SAP)
        cap   = get_col(COL_CAP)
        napv2 = get_col(COL_NAPV2)
        napv3 = get_col(COL_NAPV3)
        sapv2 = get_col(COL_SAPV2)
        sapv3 = get_col(COL_SAPV3)

        if any(v is not None for v in [nap, sap, cap, napv2, napv3, sapv2, sapv3]):
            data.append({
                'date':  date,
                'nap':   nap   if nap   is not None else 0,
                'sap':   sap   if sap   is not None else 0,
                'cap':   cap   if cap   is not None else 0,
                'napv2': napv2 if napv2 is not None else 0,
                'napv3': napv3 if napv3 is not None else 0,
                'sapv2': sapv2 if sapv2 is not None else 0,
                'sapv3': sapv3 if sapv3 is not None else 0,
            })
    
    data.sort(key=lambda x: x['date'])
    
    dates  = [d['date'].strftime('%d/%m/%y') for d in data]
    nap_l  = [d['nap']   for d in data]
    sap_l  = [d['sap']   for d in data]
    cap_l  = [d['cap']   for d in data]
    napv2_l= [d['napv2'] for d in data]
    napv3_l= [d['napv3'] for d in data]
    sapv2_l= [d['sapv2'] for d in data]
    sapv3_l= [d['sapv3'] for d in data]
    
    print(f"  ✓ Parsed {len(data)} daily records")
    if len(data) > 0:
        print(f"  Date range: {dates[0]} to {dates[-1]}")
    
    return dates, nap_l, sap_l, cap_l, napv2_l, napv3_l, sapv2_l, sapv3_l

def build_monthly_from_daily(daily_dates, nap_daily, sap_daily,
                              cap_daily, napv2_daily, napv3_daily,
                              sapv2_daily, sapv3_daily):
    """Build monthly data by aggregating daily data"""
    if not daily_dates:
        return [], [], [], [], [], [], [], []
    
    monthly_data = {}
    
    for i, date_str in enumerate(daily_dates):
        d = datetime.datetime.strptime(date_str, '%d/%m/%y')
        month_key   = d.strftime('%Y-%m')
        month_label = d.strftime('%b %y')
        
        if month_key not in monthly_data:
            monthly_data[month_key] = {
                'label': month_label,
                'nap': 0, 'sap': 0, 'cap': 0,
                'napv2': 0, 'napv3': 0, 'sapv2': 0, 'sapv3': 0
            }
        
        monthly_data[month_key]['nap']   += nap_daily[i]
        monthly_data[month_key]['sap']   += sap_daily[i]
        monthly_data[month_key]['cap']   += cap_daily[i]
        monthly_data[month_key]['napv2'] += napv2_daily[i]
        monthly_data[month_key]['napv3'] += napv3_daily[i]
        monthly_data[month_key]['sapv2'] += sapv2_daily[i]
        monthly_data[month_key]['sapv3'] += sapv3_daily[i]
    
    sorted_months = sorted(monthly_data.keys())
    months  = [monthly_data[m]['label'] for m in sorted_months]
    nap_m   = [monthly_data[m]['nap']   for m in sorted_months]
    sap_m   = [monthly_data[m]['sap']   for m in sorted_months]
    cap_m   = [monthly_data[m]['cap']   for m in sorted_months]
    napv2_m = [monthly_data[m]['napv2'] for m in sorted_months]
    napv3_m = [monthly_data[m]['napv3'] for m in sorted_months]
    sapv2_m = [monthly_data[m]['sapv2'] for m in sorted_months]
    sapv3_m = [monthly_data[m]['sapv3'] for m in sorted_months]
    
    print(f"  ✓ Built {len(months)} monthly records from daily data")
    
    return months, nap_m, sap_m, cap_m, napv2_m, napv3_m, sapv2_m, sapv3_m

def sharpe(daily, capital):
    if len(daily) < 2:
        return 0
    r = [d / capital for d in daily]
    mean_r = statistics.mean(r)
    std_r = statistics.stdev(r)
    if std_r == 0:
        return 0
    return round((mean_r / std_r) * (252 ** 0.5), 2)

def max_drawdown(daily, capital):
    cum = peak = 0
    dd = 0
    for d in daily:
        cum += d
        peak = max(peak, cum)
        dd = min(dd, (cum - peak) / capital)
    return round(dd * 100, 1)

def win_rate(daily):
    if not daily:
        return 0
    return round(sum(1 for d in daily if d > 0) / len(daily) * 100, 1)

def build_data():
    print("Fetching DAILY P&L sheet...")
    try:
        daily_csv = fetch_csv('DAILY P&L')
        daily_dates, nap_daily, sap_daily, cap_daily, napv2_daily, napv3_daily, sapv2_daily, sapv3_daily = parse_daily_pnl(daily_csv)
    except Exception as e:
        print(f"✗ Error fetching DAILY P&L: {e}")
        import traceback
        traceback.print_exc()
        daily_dates, nap_daily, sap_daily = [], [], []
        cap_daily, napv2_daily, napv3_daily, sapv2_daily, sapv3_daily = [], [], [], [], []
    
    if len(daily_dates) == 0:
        print("\n⚠️  WARNING: No daily data found!")
        print("    Please check:")
        print("    1. Sheet is published to web (File → Share → Publish to web)")
        print("    2. Sheet name is exactly 'DAILY P&L'")
        print("    3. Data format matches expected structure")
        sys.exit(1)
    
    # Build monthly data from daily aggregation
    print("\nAggregating monthly data from daily records...")
    months, nap_m, sap_m, cap_m, napv2_m, napv3_m, sapv2_m, sapv3_m = build_monthly_from_daily(
        daily_dates, nap_daily, sap_daily,
        cap_daily, napv2_daily, napv3_daily, sapv2_daily, sapv3_daily
    )
    
    # Calculate combined daily (NAP + SAP only, as before)
    comb_daily = [n + s for n, s in zip(nap_daily, sap_daily)]
    
    # Get today's date in DD/MM/YY format
    today_formatted = datetime.date.today().strftime('%d/%m/%y')
    
    data = {
        'updated': today_formatted,
        'months': months,
        'nap_monthly':   nap_m,
        'sap_monthly':   sap_m,
        'cap_monthly':   cap_m,
        'napv2_monthly': napv2_m,
        'napv3_monthly': napv3_m,
        'sapv2_monthly': sapv2_m,
        'sapv3_monthly': sapv3_m,
        'nap_daily':   nap_daily,
        'sap_daily':   sap_daily,
        'cap_daily':   cap_daily,
        'napv2_daily': napv2_daily,
        'napv3_daily': napv3_daily,
        'sapv2_daily': sapv2_daily,
        'sapv3_daily': sapv3_daily,
        'daily_dates': daily_dates,
        'stats': {
            'nap': {
                'roi':      round(sum(nap_m) / CAP_NAP * 100, 1) if nap_m else 0,
                'pnl':      round(sum(nap_m)) if nap_m else 0,
                'sharpe':   sharpe(nap_daily, CAP_NAP),
                'max_dd':   max_drawdown(nap_daily, CAP_NAP),
                'win_rate': win_rate(nap_daily),
                'days':     len(nap_daily),
            },
            'sap': {
                'roi':      round(sum(sap_m) / CAP_SAP * 100, 1) if sap_m else 0,
                'pnl':      round(sum(sap_m)) if sap_m else 0,
                'sharpe':   sharpe(sap_daily, CAP_SAP),
                'max_dd':   max_drawdown(sap_daily, CAP_SAP),
                'win_rate': win_rate(sap_daily),
                'days':     len(sap_daily),
            },
            'cap': {
                'roi':      round(sum(cap_m) / CAP_CAP * 100, 1) if cap_m else 0,
                'pnl':      round(sum(cap_m)) if cap_m else 0,
                'sharpe':   sharpe(cap_daily, CAP_CAP),
                'max_dd':   max_drawdown(cap_daily, CAP_CAP),
                'win_rate': win_rate(cap_daily),
                'days':     len([x for x in cap_daily if x != 0]),
            },
            'napv2': {
                'roi':      round(sum(napv2_m) / CAP_NAPV2 * 100, 1) if napv2_m else 0,
                'pnl':      round(sum(napv2_m)) if napv2_m else 0,
                'sharpe':   sharpe(napv2_daily, CAP_NAPV2),
                'max_dd':   max_drawdown(napv2_daily, CAP_NAPV2),
                'win_rate': win_rate(napv2_daily),
                'days':     len([x for x in napv2_daily if x != 0]),
            },
            'napv3': {
                'roi':      round(sum(napv3_m) / CAP_NAPV3 * 100, 1) if napv3_m else 0,
                'pnl':      round(sum(napv3_m)) if napv3_m else 0,
                'sharpe':   sharpe(napv3_daily, CAP_NAPV3),
                'max_dd':   max_drawdown(napv3_daily, CAP_NAPV3),
                'win_rate': win_rate(napv3_daily),
                'days':     len([x for x in napv3_daily if x != 0]),
            },
            'sapv2': {
                'roi':      round(sum(sapv2_m) / CAP_SAPV2 * 100, 1) if sapv2_m else 0,
                'pnl':      round(sum(sapv2_m)) if sapv2_m else 0,
                'sharpe':   sharpe(sapv2_daily, CAP_SAPV2),
                'max_dd':   max_drawdown(sapv2_daily, CAP_SAPV2),
                'win_rate': win_rate(sapv2_daily),
                'days':     len([x for x in sapv2_daily if x != 0]),
            },
            'sapv3': {
                'roi':      round(sum(sapv3_m) / CAP_SAPV3 * 100, 1) if sapv3_m else 0,
                'pnl':      round(sum(sapv3_m)) if sapv3_m else 0,
                'sharpe':   sharpe(sapv3_daily, CAP_SAPV3),
                'max_dd':   max_drawdown(sapv3_daily, CAP_SAPV3),
                'win_rate': win_rate(sapv3_daily),
                'days':     len([x for x in sapv3_daily if x != 0]),
            },
            'comb': {
                'roi':      round((sum(nap_m) + sum(sap_m)) / (CAP_NAP + CAP_SAP) * 100, 1) if nap_m and sap_m else 0,
                'pnl':      round(sum(nap_m) + sum(sap_m)) if nap_m and sap_m else 0,
                'sharpe':   sharpe(comb_daily, CAP_NAP + CAP_SAP),
                'max_dd':   max_drawdown(comb_daily, CAP_NAP + CAP_SAP),
                'win_rate': win_rate(comb_daily),
            },
        },
    }
    
    print(f"\n✓ Summary:")
    print(f"  - Monthly records: {len(months)}")
    print(f"  - Daily records: {len(daily_dates)}")
    print(f"  - Date range: {daily_dates[0]} to {daily_dates[-1]}")
    print(f"  - Updated date: {today_formatted}")
    print(f"  - Total NAP P&L:   ₹{sum(nap_m):,.0f}")
    print(f"  - Total SAP P&L:   ₹{sum(sap_m):,.0f}")
    print(f"  - Total CAP P&L:   ₹{sum(cap_m):,.0f}")
    print(f"  - Total NAPv2 P&L: ₹{sum(napv2_m):,.0f}")
    print(f"  - Total NAPv3 P&L: ₹{sum(napv3_m):,.0f}")
    print(f"  - Total SAPv2 P&L: ₹{sum(sapv2_m):,.0f}")
    print(f"  - Total SAPv3 P&L: ₹{sum(sapv3_m):,.0f}")
    print(f"  - Combined ROI: {data['stats']['comb']['roi']}%")
    
    return data

if __name__ == '__main__':
    try:
        data = build_data()
        with open('data.json', 'w') as f:
            json.dump(data, f, indent=2)
        print(f"\n✓ data.json written successfully!")
    except Exception as e:
        print(f"\n✗ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)
