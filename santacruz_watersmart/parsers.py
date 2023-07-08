from datetime import datetime 


DATE_FORMAT = '%Y-%m-%d'

def date_to_dt(date):
    return datetime.strptime(date, DATE_FORMAT)

def date_to_ts(date):
    return datetime.strptime(date, DATE_FORMAT).timestamp()

def date_to_iso(date):
    return datetime.strptime(date, DATE_FORMAT).isoformat()

def parse_daily_dataset(data):
    dataset = []
    for i in range(len(data['data']['chartData']['dailyData']['categories'])):
        dt = date_to_dt(data['data']['chartData']['dailyData']['categories'][i])
        row = {
            'ts': dt.timestamp(), 
            'iso_date': dt.isoformat(), 
            'categories': data['data']['chartData']['dailyData']['categories'][i], 
            'consumption': data['data']['chartData']['dailyData']['consumption'][i],
            'temperature': data['data']['chartData']['dailyData']['temperature'][i],
            'precipitation': data['data']['chartData']['dailyData']['precipitation'][i]
        }
        dataset.append(row)
    return dataset

def parse_hourly_dataset(data):
    dataset = [{
        'ts': row['read_datetime'],
        'iso_date': datetime.fromtimestamp(row['read_datetime']).isoformat(),
        'gallons': row['gallons'], 
        'leak_gallons': row['leak_gallons'], 
        'flags': row['flags']
    } for row in data['data']['series']]
    return dataset

def parse_daily_measure(d):
    return {
        'ts': d.ts,
        'iso_date': datetime.fromtimestamp(d.ts).isoformat(), 
        'categories': datetime.fromtimestamp(d.ts).strftime(DATE_FORMAT), 
        'consumption': d.consumption, 
        'temperature': d.temperature, 
        'precipitation': d.precipitation
    }

def parse_hourly_measure(d):
    return {
        'read_datetime': d.ts,
        'iso_date': datetime.fromtimestamp(d.ts).isoformat(),
        'gallons': d.consumption, 
        'leak_gallons': d.leak, 
        'flags': d.flags.split('|') if d.flags is not None else None
    }
