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
    if os.path.exists(ENRICHED_CSV):
        try:
            with open(ENRICHED_CSV, 'r', encoding='utf-8') as f:
                reader = csv.DictReader(f)
                for row in reader:
                    if not row.get('category'):
                        row['category'] = 'unit8'
                    leads.append(row)
        except Exception as e:
            print(f"Error loading enriched CSV: {e}")

    if os.path.exists(CSV_FILE):
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
                    if row.get('category') == 'office':
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

def get_stats(leads, category=None):
    if category:
        leads = [l for l in leads if l.get('category') == category]

    total = len(leads)
    with_email = sum(1 for l in leads if l.get('email') or l.get('contact_email'))
    with_contact = sum(1 for l in leads if l.get('contact_name'))
    with_phone = sum(1 for l in leads if l.get('phone'))
    with_website = sum(1 for l in leads if l.get('website'))
    enriched_complete = sum(1 for l in leads if l.get('enrichment_status') == 'complete')
    ai_enriched = sum(1 for l in leads if l.get('ai_enriched') == 'true')
    email_guessed = sum(1 for l in leads if l.get('email_guessed') == 'true')
    contact_verified = sum(1 for l in leads if l.get('contact_verified') == 'true')

    avg_score = 0
    scored = [l for l in leads if l.get('ai_score')]
    if scored:
        try:
            avg_score = sum(int(l['ai_score']) for l in scored) / len(scored)
        except:
            pass

    pct_complete = round((enriched_complete / total * 100), 1) if total > 0 else 0

    name_review = sum(1 for l in leads if l.get('name_review_needed') == 'True')
    missing_email_count = sum(1 for l in leads if l.get('missing_email') == 'True')
    
    confidence_scores = [int(l['confidence_score']) for l in leads if l.get('confidence_score') and l['confidence_score'].isdigit()]
    avg_confidence = round(sum(confidence_scores) / len(confidence_scores), 1) if confidence_scores else 0
    
    email_types = {}
    for l in leads:
        et = l.get('email_type', 'none') or 'none'
        email_types[et] = email_types.get(et, 0) + 1

    return {
        'total': total,
        'with_email': with_email,
        'with_contact': with_contact,
        'with_phone': with_phone,
        'with_website': with_website,
        'enriched_complete': enriched_complete,
        'ai_enriched': ai_enriched,
        'email_guessed': email_guessed,
        'contact_verified': contact_verified,
        'avg_score': round(avg_score, 1),
        'pct_complete': pct_complete,
        'name_review': name_review,
        'missing_email': missing_email_count,
        'avg_confidence': avg_confidence,
        'email_types': email_types
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

    min_score = request.args.get('min_score')

    if category == 'unit8':
        leads = [l for l in leads if l.get('category') == 'unit8']
        filename = 'unit8_leads.csv'
    elif category == 'office':
        leads = [l for l in leads if l.get('category') == 'office']
        filename = 'office_leads.csv'
    else:
        filename = 'all_leads.csv'

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
    fieldnames = [
        'company_name', 'website', 'website_verified', 'facebook_url',
        'contact_name', 'contact_names',
        'contact_email', 'personal_email_guesses', 'team_email_guesses',
        'principal_name', 'principal_email_guess',
        'generic_email', 'email_type',
        'name_review_needed', 'missing_email',
        'data_score', 'confidence_score',
        'sector', 'location', 'phone', 'linkedin',
        'ai_score', 'ai_reason', 'tag', 'google_rating',
        'category', 'place_id', 'search_town',
        'enrichment_source', 'enrichment_status',
        'enrichment_attempts', 'refinement_notes'
    ]
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
                    stats['with_contact'] = sum(1 for r in rows if r.get('contact_name'))
                    stats['with_personal_email'] = sum(1 for r in rows if r.get('personal_email_guesses'))
                    stats['with_generic_email'] = sum(1 for r in rows if r.get('generic_email'))
                    stats['with_team_guesses'] = sum(1 for r in rows if r.get('team_email_guesses'))
                    stats['name_review'] = sum(1 for r in rows if r.get('name_review_needed') == 'True')
                    stats['missing_email'] = sum(1 for r in rows if r.get('missing_email') == 'True')
                    stats['website_verified'] = sum(1 for r in rows if r.get('website_verified', '').lower() in ('yes', 'facebook'))
                    stats['facebook_only'] = sum(1 for r in rows if r.get('website_verified') == 'facebook')
                elif label == 'excluded':
                    stats['exclusion_reason'] = 'no website and no Facebook page'
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
        'openai': get_openai_stats()
    })

if __name__ == '__main__':
    app.run(host='0.0.0.0', port=5000, debug=True)
