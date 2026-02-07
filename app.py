import os
import csv
import io
import json
from flask import Flask, render_template, jsonify, request, Response

app = Flask(__name__)
app.secret_key = os.environ.get('SESSION_SECRET', 'dev-secret-key')

CSV_FILE = 'leads.csv'
ENRICHED_CSV = 'unit8_leads_enriched.csv'
EXCLUDED_CSV = 'unit8_leads_excluded.csv'
OPENAI_COST_FILE = '/tmp/openai_enrichment_cost.json'
PLACES_API_FILE = '/tmp/places_api_stats.json'

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

def get_openai_stats():
    try:
        if os.path.exists(OPENAI_COST_FILE):
            with open(OPENAI_COST_FILE, 'r') as f:
                data = json.load(f)
                return {
                    'date': data.get('date', ''),
                    'cost': round(data.get('cost', 0), 4),
                    'calls': data.get('calls', 0),
                    'limit': 2.00
                }
    except:
        pass
    return {'date': '', 'cost': 0, 'calls': 0, 'limit': 2.00}

def get_places_api_stats():
    try:
        if os.path.exists(PLACES_API_FILE):
            with open(PLACES_API_FILE, 'r') as f:
                data = json.load(f)
                return {
                    'date': data.get('date', ''),
                    'calls': data.get('calls', 0)
                }
    except:
        pass
    return {'date': '', 'calls': 0}

def get_stats(leads, category=None):
    if category:
        leads = [l for l in leads if l.get('category') == category]
    
    total = len(leads)
    with_email = sum(1 for l in leads if l.get('email'))
    with_contact = sum(1 for l in leads if l.get('contact_name'))
    with_phone = sum(1 for l in leads if l.get('phone'))
    with_website = sum(1 for l in leads if l.get('website'))
    enriched_complete = sum(1 for l in leads if l.get('enrichment_status') == 'complete')
    enriched_incomplete = sum(1 for l in leads if l.get('enrichment_status') == 'incomplete')
    missing_email = sum(1 for l in leads if l.get('enrichment_status') == 'missing_email')
    missing_name = sum(1 for l in leads if l.get('enrichment_status') == 'missing_name')
    ai_enriched = sum(1 for l in leads if l.get('ai_enriched') == 'true')
    email_guessed = sum(1 for l in leads if l.get('email_guessed') == 'true')
    contact_verified = sum(1 for l in leads if l.get('contact_verified') == 'true')
    
    sources = {}
    for l in leads:
        source = l.get('enrichment_source', 'not_found') or 'not_found'
        sources[source] = sources.get(source, 0) + 1
    
    avg_score = 0
    scored = [l for l in leads if l.get('ai_score')]
    if scored:
        try:
            avg_score = sum(int(l['ai_score']) for l in scored) / len(scored)
        except:
            pass
    
    pct_complete = round((enriched_complete / total * 100), 1) if total > 0 else 0
    pct_guessed = round((email_guessed / with_email * 100), 1) if with_email > 0 else 0
    pct_verified = round((contact_verified / with_contact * 100), 1) if with_contact > 0 else 0
    
    return {
        'total': total,
        'with_email': with_email,
        'with_contact': with_contact,
        'with_phone': with_phone,
        'with_website': with_website,
        'enriched_complete': enriched_complete,
        'enriched_incomplete': enriched_incomplete,
        'missing_email': missing_email,
        'missing_name': missing_name,
        'ai_enriched': ai_enriched,
        'email_guessed': email_guessed,
        'contact_verified': contact_verified,
        'sources': sources,
        'avg_score': round(avg_score, 1),
        'pct_complete': pct_complete,
        'pct_guessed': pct_guessed,
        'pct_verified': pct_verified
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
                         total_stats=get_stats(leads),
                         openai_stats=get_openai_stats())

@app.route('/api/leads')
def api_leads():
    leads = load_leads()
    
    category = request.args.get('category')
    min_score = request.args.get('min_score')
    search = request.args.get('search', '').lower()
    status_filter = request.args.get('status')
    source_filter = request.args.get('source')
    
    if category:
        leads = [l for l in leads if l.get('category') == category]
    
    if min_score:
        try:
            min_val = int(min_score)
            leads = [l for l in leads if l.get('ai_score') and int(l['ai_score']) >= min_val]
        except:
            pass
    
    if status_filter:
        if status_filter == 'incomplete':
            leads = [l for l in leads if l.get('enrichment_status') != 'complete']
        elif status_filter == 'complete':
            leads = [l for l in leads if l.get('enrichment_status') == 'complete']
        elif status_filter == 'missing_email':
            leads = [l for l in leads if l.get('enrichment_status') == 'missing_email']
        elif status_filter == 'missing_name':
            leads = [l for l in leads if l.get('enrichment_status') == 'missing_name']
    
    if source_filter:
        leads = [l for l in leads if l.get('enrichment_source') == source_filter]
    
    if search:
        leads = [l for l in leads if search in l.get('company_name', '').lower() 
                 or search in l.get('sector', '').lower()
                 or search in l.get('location', '').lower()]
    
    return jsonify({'leads': leads, 'stats': get_stats(leads)})

@app.route('/api/download/<category>')
def download_csv(category):
    leads = load_leads()
    
    status_filter = request.args.get('status')
    source_filter = request.args.get('source')
    min_score = request.args.get('min_score')
    
    if category == 'unit8':
        leads = [l for l in leads if l.get('category') == 'unit8']
        filename = 'unit8_leads.csv'
    elif category == 'office':
        leads = [l for l in leads if l.get('category') == 'office']
        filename = 'office_leads.csv'
    else:
        filename = 'all_leads.csv'
    
    if status_filter:
        if status_filter == 'incomplete':
            leads = [l for l in leads if l.get('enrichment_status') != 'complete']
            filename = f'{status_filter}_{filename}'
        elif status_filter == 'complete':
            leads = [l for l in leads if l.get('enrichment_status') == 'complete']
            filename = f'{status_filter}_{filename}'
        elif status_filter == 'missing_email':
            leads = [l for l in leads if l.get('enrichment_status') == 'missing_email']
            filename = f'{status_filter}_{filename}'
        elif status_filter == 'missing_name':
            leads = [l for l in leads if l.get('enrichment_status') == 'missing_name']
            filename = f'{status_filter}_{filename}'
    
    if source_filter:
        leads = [l for l in leads if l.get('enrichment_source') == source_filter]
        filename = f'{source_filter}_{filename}'
    
    if min_score:
        try:
            min_val = int(min_score)
            leads = [l for l in leads if l.get('ai_score') and int(l['ai_score']) >= min_val]
            filename = f'score{min_val}plus_{filename}'
        except:
            pass
    
    if not leads:
        return Response("No leads found", status=404)
    
    output = io.StringIO()
    fieldnames = ['company_name', 'sector', 'location', 'website', 'website_verified',
                  'principal_name', 'principal_email_guess',
                  'contact_name', 'contact_name_validity',
                  'contact_names', 'generic_email', 'email', 'guessed_personal_emails',
                  'personal_email_guesses', 'email_type',
                  'contact_titles', 'multiple_contacts',
                  'phone', 'linkedin', 'ai_score', 'ai_reason', 'tag', 
                  'google_rating', 'data_score', 'archived', 'category',
                  'place_id', 'search_town', 
                  'enrichment_source', 'enrichment_status', 'ai_enriched', 
                  'email_guessed', 'contact_verified']
    writer = csv.DictWriter(output, fieldnames=fieldnames, extrasaction='ignore')
    writer.writeheader()
    writer.writerows(leads)
    
    return Response(
        output.getvalue(),
        mimetype='text/csv',
        headers={'Content-Disposition': f'attachment; filename={filename}'}
    )

@app.route('/api/download/enriched')
def download_enriched():
    if not os.path.exists(ENRICHED_CSV):
        return Response("Enriched CSV not found. Run refine_leads.py first.", status=404)
    with open(ENRICHED_CSV, 'r', encoding='utf-8') as f:
        content = f.read()
    return Response(
        content,
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=unit8_leads_enriched.csv'}
    )

@app.route('/api/download/excluded')
def download_excluded():
    if not os.path.exists(EXCLUDED_CSV):
        return Response("Excluded CSV not found. Run refine_leads.py first.", status=404)
    with open(EXCLUDED_CSV, 'r', encoding='utf-8') as f:
        content = f.read()
    return Response(
        content,
        mimetype='text/csv',
        headers={'Content-Disposition': 'attachment; filename=unit8_leads_excluded.csv'}
    )

@app.route('/api/refinement-stats')
def refinement_stats():
    stats = {}
    for label, path in [('enriched', ENRICHED_CSV), ('excluded', EXCLUDED_CSV)]:
        if os.path.exists(path):
            with open(path, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                rows = list(reader)
                stats[label] = len(rows)
                if label == 'enriched':
                    stats['high'] = sum(1 for r in rows if r.get('data_score') == 'high')
                    stats['medium'] = sum(1 for r in rows if r.get('data_score') == 'medium')
                    stats['low'] = sum(1 for r in rows if r.get('data_score') == 'low')
                    stats['with_principal'] = sum(1 for r in rows if r.get('principal_name'))
                    stats['with_principal_email'] = sum(1 for r in rows if r.get('principal_email_guess'))
                    stats['with_contact'] = sum(1 for r in rows if r.get('contact_name_validity') == 'valid')
                    stats['with_personal_email'] = sum(1 for r in rows if r.get('email_type') in ('personal', 'both'))
                    stats['with_generic_email'] = sum(1 for r in rows if r.get('generic_email'))
                    stats['with_guesses'] = sum(1 for r in rows if r.get('guessed_personal_emails'))
                    stats['website_verified'] = sum(1 for r in rows if r.get('website_verified', '').lower() in ('yes', 'facebook'))
                    stats['archived_false'] = sum(1 for r in rows if r.get('archived') == 'FALSE')
                elif label == 'excluded':
                    reasons = {}
                    for r in rows:
                        reason = r.get('excluded_reason', 'unknown')
                        parts = [p.strip() for p in reason.replace('|', ';').split(';') if p.strip()]
                        for part in parts:
                            reasons[part] = reasons.get(part, 0) + 1
                    stats['exclusion_reasons'] = reasons
        else:
            stats[label] = 0
    return jsonify(stats)

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

@app.route('/api/stats')
def api_stats():
    leads = load_leads()
    return jsonify({
        'total': get_stats(leads),
        'unit8': get_stats(leads, 'unit8'),
        'office': get_stats(leads, 'office'),
        'openai': get_openai_stats(),
        'places_api': get_places_api_stats()
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
