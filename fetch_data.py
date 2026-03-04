"""
Fetches Apex Algos P&L data from Google Sheets and writes data.json.
CUSTOM VERSION for your specific data format (DD-MM-YY dates, ₹ symbols)
"""
import requests, json, csv, io, datetime, statistics, sys
import re

SHEET_ID  = '1Rgy-HH8bcY7guN7PuH6biiK-JVV-PaFOH5aDII8oVf8'
CAP_NAP   = 250_000
CAP_SAP   = 250_000

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
    
    # Skip first 2 rows (headers and capital row)
    for i, row in enumerate(rows[2:], start=2):
        if not row or not row[0].strip():
            continue
        
        # Parse date from first column (DD-MM-YY format)
        date = parse_date_ddmmyy(row[0])
        if not date:
            print(f"  Skipping row {i}: couldn't parse date '{row[0]}'")
            continue
        
        # Column structure from your CSV:
        # Col 0: Date (DD-MM-YY)
        # Col 1: NAP P&L (with ₹ symbol)
        # Col 4: SAP P&L (with ₹ symbol)
        
        nap = clean_number(row[1]) if len(row) > 1 else None
        sap = clean_number(row[4]) if len(row) > 4 else None
        
        if nap is not None or sap is not None:
            data.append({
                'date': date,
                'nap': nap if nap is not None else 0,
                'sap': sap if sap is not None else 0
            })
    
    # Sort by date (oldest first)
    data.sort(key=lambda x: x['date'])
    
    # Extract into separate lists with DD/MM/YY format for display
    dates = [d['date'].strftime('%d/%m/%y') for d in data]
    nap_list = [d['nap'] for d in data]
    sap_list = [d['sap'] for d in data]
    
    print(f"  ✓ Parsed {len(data)} daily records")
    if len(data) > 0:
        print(f"  Date range: {dates[0]} to {dates[-1]}")
    
    return dates, nap_list, sap_list

def build_monthly_from_daily(daily_dates, nap_daily, sap_daily):
    """Build monthly data by aggregating daily data"""
    if not daily_dates:
        return [], [], []
    
    monthly_data = {}
    
    # Group by month
    for i, date_str in enumerate(daily_dates):
        # Parse DD/MM/YY to get month
        d = datetime.datetime.strptime(date_str, '%d/%m/%y')
        month_key = d.strftime('%Y-%m')
        month_label = d.strftime('%b %y')
        
        if month_key not in monthly_data:
            monthly_data[month_key] = {
                'label': month_label,
                'nap': 0,
                'sap': 0
            }
        
        monthly_data[month_key]['nap'] += nap_daily[i]
        monthly_data[month_key]['sap'] += sap_daily[i]
    
    # Sort by month and extract
    sorted_months = sorted(monthly_data.keys())
    months = [monthly_data[m]['label'] for m in sorted_months]
    nap_m = [monthly_data[m]['nap'] for m in sorted_months]
    sap_m = [monthly_data[m]['sap'] for m in sorted_months]
    
    print(f"  ✓ Built {len(months)} monthly records from daily data")
    
    return months, nap_m, sap_m

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
        daily_dates, nap_daily, sap_daily = parse_daily_pnl(daily_csv)
    except Exception as e:
        print(f"✗ Error fetching DAILY P&L: {e}")
        import traceback
        traceback.print_exc()
        daily_dates, nap_daily, sap_daily = [], [], []
    
    if len(daily_dates) == 0:
        print("\n⚠️  WARNING: No daily data found!")
        print("    Please check:")
        print("    1. Sheet is published to web (File → Share → Publish to web)")
        print("    2. Sheet name is exactly 'DAILY P&L'")
        print("    3. Data format matches expected structure")
        sys.exit(1)
    
    # Build monthly data from daily aggregation
    print("\nAggregating monthly data from daily records...")
    months, nap_m, sap_m = build_monthly_from_daily(daily_dates, nap_daily, sap_daily)
    
    # Calculate combined daily
    comb_daily = [n + s for n, s in zip(nap_daily, sap_daily)]
    
    # Get today's date in DD/MM/YY format
    today_formatted = datetime.date.today().strftime('%d/%m/%y')
    
    data = {
        'updated': today_formatted,
        'months': months,
        'nap_monthly': nap_m,
        'sap_monthly': sap_m,
        'nap_daily': nap_daily,
        'sap_daily': sap_daily,
        'daily_dates': daily_dates,
        'stats': {
            'nap': {
                'roi': round(sum(nap_m) / CAP_NAP * 100, 1) if nap_m else 0,
                'pnl': round(sum(nap_m)) if nap_m else 0,
                'sharpe': sharpe(nap_daily, CAP_NAP),
                'max_dd': max_drawdown(nap_daily, CAP_NAP),
                'win_rate': win_rate(nap_daily),
                'days': len(nap_daily),
            },
            'sap': {
                'roi': round(sum(sap_m) / CAP_SAP * 100, 1) if sap_m else 0,
                'pnl': round(sum(sap_m)) if sap_m else 0,
                'sharpe': sharpe(sap_daily, CAP_SAP),
                'max_dd': max_drawdown(sap_daily, CAP_SAP),
                'win_rate': win_rate(sap_daily),
                'days': len(sap_daily),
            },
            'comb': {
                'roi': round((sum(nap_m) + sum(sap_m)) / (CAP_NAP + CAP_SAP) * 100, 1) if nap_m and sap_m else 0,
                'pnl': round(sum(nap_m) + sum(sap_m)) if nap_m and sap_m else 0,
                'sharpe': sharpe(comb_daily, CAP_NAP + CAP_SAP),
                'max_dd': max_drawdown(comb_daily, CAP_NAP + CAP_SAP),
                'win_rate': win_rate(comb_daily),
            },
        },
    }
    
    print(f"\n✓ Summary:")
    print(f"  - Monthly records: {len(months)}")
    print(f"  - Daily records: {len(daily_dates)}")
    print(f"  - Date range: {daily_dates[0]} to {daily_dates[-1]}")
    print(f"  - Updated date: {today_formatted}")
    print(f"  - Total NAP P&L: ₹{sum(nap_m):,.0f}")
    print(f"  - Total SAP P&L: ₹{sum(sap_m):,.0f}")
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
