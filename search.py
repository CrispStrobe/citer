#!/usr/bin/env python3
# search.py
"""
Command-line tool for searching SRU and IxTheo endpoints.
"""

import argparse
import sys
import json
import logging
from lib.sru_client import SRUClient, BiblioRecord
from lib.ixtheo_client import IxTheoSearchHandler

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    stream=sys.stderr
)
logger = logging.getLogger("search")

# SRU endpoints (all verified live 2026-07-03). search_sru takes a raw CQL query,
# so the example index syntax for each endpoint is noted in the comments.
SRU_ENDPOINTS = {
    'dnb': {  # TIT= / PER= / ISBN=
        'name': 'Deutsche Nationalbibliothek',
        'url': 'https://services.dnb.de/sru/dnb',
        'default_schema': 'RDFxml',
    },
    'bnf': {  # bib.title any "..." / bib.author any "..."
        'name': 'Bibliothèque nationale de France',
        'url': 'http://catalogue.bnf.fr/api/SRU',
        'default_schema': 'dublincore',
    },
    'zdb': {  # TIT= / ISS= (serials)
        'name': 'ZDB - German Union Catalogue of Serials',
        'url': 'https://services.dnb.de/sru/zdb',
        'default_schema': 'MARC21-xml',
    },
    'loc': {  # bath.title= / bath.author=  (port 210 gateway)
        'name': 'Library of Congress',
        'url': 'http://lx2.loc.gov:210/lcdb',
        'default_schema': 'marcxml',
    },
    'kb': {  # dc.title= / dc.creator=  (GGC collection via x-collection)
        'name': 'KB - National Library of the Netherlands',
        'url': 'http://jsru.kb.nl/sru/sru',
        'default_schema': 'dc',
        'query_params': {'x-collection': 'GGC'},
    },
    'bibsys': {  # alma.title= / alma.creator=
        'name': 'BIBSYS - Norwegian Academic Libraries',
        'url': 'https://bibsys-network.alma.exlibrisgroup.com/view/sru/47BIBSYS_NETWORK',
        'default_schema': 'marcxml',
    },
    'k10plus': {  # pica.tit= / pica.per= / pica.isb=
        'name': 'K10plus (GBV + SWB union catalogue)',
        'url': 'https://sru.k10plus.de/opac-de-627',
        'default_schema': 'marcxml',
    },
    'swisscovery': {  # alma.title= / alma.creator=
        'name': 'swisscovery (SLSP, Swiss academic union)',
        'url': 'https://swisscovery.slsp.ch/view/sru/41SLSP_NETWORK',
        'default_schema': 'marcxml',
    },
}

IXTHEO_ENDPOINTS = {
    "ixtheo": {
        "name": "Index Theologicus (IxTheo)",
    }
}

def list_endpoints(protocol=None):
    """Display information about available endpoints."""
    if protocol and protocol not in ['sru', 'ixtheo']:
        logger.error(f"Unknown protocol: {protocol}")
        logger.info("Valid protocols are: sru, ixtheo")
        return
    
    if not protocol or protocol == 'sru':
        print("\nAvailable SRU Endpoints:\n")
        print(f"{'ID':<10} {'Name':<40}")
        print("-" * 50)
        
        for id, info in SRU_ENDPOINTS.items():
            print(f"{id:<10} {info['name']:<40}")
    
    if not protocol or protocol == 'ixtheo':
        print("\nAvailable IxTheo Endpoints:\n")
        print(f"{'ID':<10} {'Name':<40}")
        print("-" * 50)
        
        for id, info in IXTHEO_ENDPOINTS.items():
            print(f"{id:<10} {info['name']:<40}")

def format_record(record, format_type='text'):
    if format_type == 'json':
        return json.dumps(record.to_dict(), indent=2)
    
    result = []
    result.append(f"Title: {record.title}")
    if record.authors:
        result.append(f"Author(s): {', '.join(record.authors)}")
    if record.year:
        result.append(f"Year: {record.year}")
    if record.publisher_name:
        result.append(f"Publisher: {record.publisher_name}")
    if record.isbn:
        result.append(f"ISBN: {record.isbn}")
    if record.issn:
        result.append(f"ISSN: {record.issn}")
    return "\n".join(result)

def search_sru(args):
    endpoint_id = args.endpoint
    if endpoint_id not in SRU_ENDPOINTS:
        logger.error(f"Unknown SRU endpoint: {endpoint_id}")
        return False, []
    
    endpoint_info = SRU_ENDPOINTS[endpoint_id]
    
    query = args.query
    
    client = SRUClient(
        base_url=endpoint_info['url'],
        default_schema=endpoint_info.get('default_schema'),
        query_params=endpoint_info.get('query_params'),
    )
    
    total, records = client.search(query, max_records=args.max_records)
    
    if not records:
        logger.warning("No results found")
        return False, []
        
    return True, records

def search_ixtheo(args):
    handler = IxTheoSearchHandler()
    total, records = handler.search(query=args.query, max_results=args.max_records)
    
    if not records:
        logger.warning("No results found")
        return False, []
        
    return True, records

def main():
    parser = argparse.ArgumentParser(description='Search library endpoints.')
    parser.add_argument('--protocol', choices=['sru', 'ixtheo'], default='sru', help='Protocol to use')
    parser.add_argument('--endpoint', default='dnb', help='Endpoint to search')
    parser.add_argument('--query', help='Search query')
    parser.add_argument('--max-records', type=int, default=10, help='Maximum number of records to return')
    parser.add_argument('--format', choices=['text', 'json'], default='text', help='Output format')
    parser.add_argument('--list', action='store_true', help='List available endpoints')
    
    args = parser.parse_args()
    
    if args.list:
        list_endpoints(args.protocol)
        sys.exit(0)
        
    if not args.query:
        logger.error("No query specified.")
        sys.exit(1)
        
    if args.protocol == 'sru':
        success, records = search_sru(args)
    elif args.protocol == 'ixtheo':
        success, records = search_ixtheo(args)
    else:
        logger.error(f"Unknown protocol: {args.protocol}")
        sys.exit(1)
        
    if success:
        for record in records:
            print(format_record(record, args.format))
            print("---")

if __name__ == "__main__":
    main()
