import json
import csv

# Load oracle-cards.json (downloaded from Scryfall)
with open('.venv/Scripts/oracle-cards.json', 'r', encoding='utf-8') as f:
    data = json.load(f)

# Fields to extract
fields = ['name', 'mana_cost', 'cmc', 'type_line', 'oracle_text', 'power', 'toughness']

# Write to CSV
with open('.venv/Scripts/scryfall_slimmed.csv', 'w', newline='', encoding='utf-8') as csvfile:
    writer = csv.DictWriter(csvfile, fieldnames=fields)
    writer.writeheader()
    for card in data:
        writer.writerow({
            'name': card.get('name'),
            'mana_cost': card.get('mana_cost'),
            'cmc': card.get('cmc'),
            'type_line': card.get('type_line'),
            'oracle_text': card.get('oracle_text'),
            'power': card.get('power'),
            'toughness': card.get('toughness'),
        })