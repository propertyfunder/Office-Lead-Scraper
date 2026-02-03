import os
import csv
from flask import Flask, render_template, jsonify, request

app = Flask(__name__)
app.secret_key = os.environ.get('SESSION_SECRET', 'dev-secret-key')

CSV_FILE = 'leads.csv'

def load_leads():
    leads = []
    if not os.path.exists(CSV_FILE):
        return leads
    
    try:
        with open(CSV_FILE, 'r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            for row in reader:
                leads.append(row)
    except Exception as e:
        print(f"Error loading CSV: {e}")
    
    return leads

def get_stats(leads):
    total = len(leads)
    wellness_count = sum(1 for l in leads if l.get('tag') in ['wellness', 'clinic-target'])
    clinic_count = sum(1 for l in leads if l.get('tag') == 'clinic-target')
    with_email = sum(1 for l in leads if l.get('email'))
    with_phone = sum(1 for l in leads if l.get('phone'))
    with_website = sum(1 for l in leads if l.get('website'))
    
    avg_score = 0
    scored = [l for l in leads if l.get('ai_score')]
    if scored:
        try:
            avg_score = sum(int(l['ai_score']) for l in scored) / len(scored)
        except:
            pass
    
    return {
        'total': total,
        'wellness': wellness_count,
        'clinic': clinic_count,
        'with_email': with_email,
        'with_phone': with_phone,
        'with_website': with_website,
        'avg_score': round(avg_score, 1)
    }

@app.route('/')
def index():
    leads = load_leads()
    stats = get_stats(leads)
    return render_template('index.html', leads=leads, stats=stats)

@app.route('/api/leads')
def api_leads():
    leads = load_leads()
    
    tag_filter = request.args.get('tag')
    min_score = request.args.get('min_score')
    search = request.args.get('search', '').lower()
    
    if tag_filter:
        leads = [l for l in leads if l.get('tag') == tag_filter]
    
    if min_score:
        try:
            min_val = int(min_score)
            leads = [l for l in leads if l.get('ai_score') and int(l['ai_score']) >= min_val]
        except:
            pass
    
    if search:
        leads = [l for l in leads if search in l.get('company_name', '').lower() 
                 or search in l.get('sector', '').lower()
                 or search in l.get('location', '').lower()]
    
    return jsonify({'leads': leads, 'stats': get_stats(leads)})

@app.route('/api/refresh')
def api_refresh():
    leads = load_leads()
    stats = get_stats(leads)
    return jsonify({'leads': leads, 'stats': stats})

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
