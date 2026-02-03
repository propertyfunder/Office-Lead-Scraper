import os
import csv
import io
from flask import Flask, render_template, jsonify, request, Response

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
                if not row.get('category'):
                    tag = row.get('tag', '').lower()
                    if 'wellness' in tag or 'clinic' in tag:
                        row['category'] = 'unit8'
                    else:
                        row['category'] = 'office'
                leads.append(row)
    except Exception as e:
        print(f"Error loading CSV: {e}")
    
    return leads

def get_stats(leads, category=None):
    if category:
        leads = [l for l in leads if l.get('category') == category]
    
    total = len(leads)
    with_email = sum(1 for l in leads if l.get('email'))
    with_contact = sum(1 for l in leads if l.get('contact_name'))
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
        'with_email': with_email,
        'with_contact': with_contact,
        'with_phone': with_phone,
        'with_website': with_website,
        'avg_score': round(avg_score, 1)
    }

@app.route('/')
def index():
    leads = load_leads()
    unit8_leads = [l for l in leads if l.get('category') == 'unit8']
    office_leads = [l for l in leads if l.get('category') == 'office']
    
    return render_template('index.html', 
                         unit8_leads=unit8_leads,
                         office_leads=office_leads,
                         unit8_stats=get_stats(leads, 'unit8'),
                         office_stats=get_stats(leads, 'office'),
                         total_stats=get_stats(leads))

@app.route('/api/leads')
def api_leads():
    leads = load_leads()
    
    category = request.args.get('category')
    min_score = request.args.get('min_score')
    search = request.args.get('search', '').lower()
    
    if category:
        leads = [l for l in leads if l.get('category') == category]
    
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

@app.route('/api/download/<category>')
def download_csv(category):
    leads = load_leads()
    
    if category == 'unit8':
        leads = [l for l in leads if l.get('category') == 'unit8']
        filename = 'unit8_leads.csv'
    elif category == 'office':
        leads = [l for l in leads if l.get('category') == 'office']
        filename = 'office_leads.csv'
    else:
        filename = 'all_leads.csv'
    
    if not leads:
        return Response("No leads found", status=404)
    
    output = io.StringIO()
    fieldnames = ['company_name', 'sector', 'location', 'website', 'contact_name', 
                  'email', 'phone', 'ai_score', 'ai_reason', 'category', 'google_rating']
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(leads)
    
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename={filename}'}
    )

@app.route('/api/refresh')
def api_refresh():
    leads = load_leads()
    unit8_leads = [l for l in leads if l.get('category') == 'unit8']
    office_leads = [l for l in leads if l.get('category') == 'office']
    return jsonify({
        'unit8_leads': unit8_leads,
        'office_leads': office_leads,
        'unit8_stats': get_stats(leads, 'unit8'),
        'office_stats': get_stats(leads, 'office')
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
