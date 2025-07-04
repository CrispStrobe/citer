#!/usr/bin/env python3
# sru_library.py
"""
SRU Library - A flexible SRU (Search/Retrieve via URL) client for bibliographic data

This module provides a modular approach to query various library SRU endpoints
without requiring hardcoded classes for each specific library.
"""

import requests
import xml.etree.ElementTree as ET
import urllib.parse
import logging
import time
from dataclasses import dataclass, field
from typing import Dict, List, Any, Optional, Union, Tuple, Callable, Set
import re

# Configure logging
from lib import logger

@dataclass
class BiblioRecord:
    """Data class for bibliographic records."""
    id: str
    title: str
    # Separate authors and editors
    authors: List[str] = field(default_factory=list)
    editors: List[str] = field(default_factory=list)  # New field for editors
    translators: List[str] = field(default_factory=list)  # New field for translators
    contributors: List[Dict[str, str]] = field(default_factory=list)  # People with specific roles
    year: Optional[str] = None
    publisher_name: Optional[str] = None
    place_of_publication: Optional[str] = None
    isbn: Optional[str] = None
    issn: Optional[str] = None
    urls: List[str] = field(default_factory=list)
    abstract: Optional[str] = None
    language: Optional[str] = None
    format: Optional[str] = None
    subjects: List[str] = field(default_factory=list)
    series: Optional[str] = None
    extent: Optional[str] = None  # Number of pages, duration, etc.
    edition: Optional[str] = None
    raw_data: Any = None
    schema: Optional[str] = None  # Store the schema used for parsing
    
    # Journal related fields
    journal_title: Optional[str] = None  # For journal articles 
    volume: Optional[str] = None  # Volume number
    issue: Optional[str] = None   # Issue number
    pages: Optional[str] = None   # Page range
    doi: Optional[str] = None     # Digital Object Identifier
    document_type: Optional[str] = None  # Article, Book, etc.
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert record to dictionary."""
        return {
            "id": self.id,
            "title": self.title,
            "authors": self.authors,
            "editors": self.editors,
            "translators": self.translators,
            "contributors": self.contributors,
            "year": self.year,
            "publisher_name": self.publisher_name,
            "place_of_publication": self.place_of_publication,
            "isbn": self.isbn,
            "issn": self.issn,
            "urls": self.urls,
            "abstract": self.abstract,
            "language": self.language,
            "format": self.format,
            "subjects": self.subjects,
            "series": self.series,
            "extent": self.extent,
            "edition": self.edition,
            "journal_title": self.journal_title,
            "volume": self.volume,
            "issue": self.issue,
            "pages": self.pages,
            "doi": self.doi,
            "document_type": self.document_type,
            "schema": self.schema
        }
    
    def __str__(self) -> str:
        """String representation of the record."""
        authors_str = ", ".join(self.authors) if self.authors else ""
        editors_str = ", ".join(self.editors) if self.editors else ""
        credit_parts = []
        if authors_str:
            credit_parts.append(f"by {authors_str}")
        if editors_str:
            credit_parts.append(f"edited by {editors_str}")
        
        credit = " ".join(credit_parts) if credit_parts else "Unknown"
        year_str = self.year or "n.d."
        
        # For journal articles
        if self.journal_title:
            journal_info = self.journal_title
            if self.volume:
                journal_info += f", {self.volume}"
                if self.issue:
                    journal_info += f"({self.issue})"
            if self.pages:
                journal_info += f", pp. {self.pages}"
            return f"{self.title} {credit} ({year_str}, {journal_info})"
        
        # For books and other materials
        pub_info = []
        if self.place_of_publication:
            pub_info.append(self.place_of_publication)
        if self.publisher_name:
            pub_info.append(self.publisher_name)
        
        pub_str = ": ".join(pub_info) if pub_info else "Unknown"
        
        return f"{self.title} {credit} ({year_str}, {pub_str})"

    def get_citation_key(self) -> str:
        """Generate a sensible BibTeX citation key."""
        # Get first author's last name or 'unknown' if no authors
        if self.authors:
            # Extract last name (after the last comma or the whole name if no comma)
            first_author = self.authors[0]
            
            # Clean the name first - remove any role indicators
            first_author = re.sub(r'\s*\[[^\]]*\]', '', first_author)
            # Fix broken brackets
            first_author = re.sub(r'\]\s*$', '', first_author)
            first_author = re.sub(r'^\s*\[', '', first_author)
            
            if ',' in first_author:
                author_key = first_author.split(',')[0].strip().lower()
            else:
                # Take the last word as the last name
                parts = first_author.split()
                author_key = parts[-1].lower() if parts else 'unknown'
        elif self.editors:
            # Use first editor if no authors
            first_editor = self.editors[0]
            
            # Clean the name first
            first_editor = re.sub(r'\s*\[[^\]]*\]', '', first_editor)
            # Fix broken brackets
            first_editor = re.sub(r'\]\s*$', '', first_editor)
            first_editor = re.sub(r'^\s*\[', '', first_editor)
            
            if ',' in first_editor:
                author_key = first_editor.split(',')[0].strip().lower()
            else:
                parts = first_editor.split()
                author_key = parts[-1].lower() if parts else 'editor'
        else:
            author_key = 'unknown'
        
        # Remove any non-alphanumeric characters
        author_key = re.sub(r'[^a-z0-9]', '', author_key)
        
        # If author_key is empty after cleaning, use 'unknown'
        if not author_key:
            author_key = 'unknown'
        
        # Add year if available
        if self.year:
            return f"{author_key}{self.year}"
        return author_key
    
    def get_citation_key_old(self) -> str:
        """Generate a sensible BibTeX citation key."""
        # Get first author's last name or 'unknown' if no authors
        if self.authors:
            # Extract last name (after the last comma or the whole name if no comma)
            first_author = self.authors[0]
            if ',' in first_author:
                author_key = first_author.split(',')[0].strip().lower()
            else:
                # Take the last word as the last name
                parts = first_author.split()
                author_key = parts[-1].lower() if parts else 'unknown'
        elif self.editors:
            # Use first editor if no authors
            first_editor = self.editors[0]
            if ',' in first_editor:
                author_key = first_editor.split(',')[0].strip().lower()
            else:
                parts = first_editor.split()
                author_key = parts[-1].lower() if parts else 'editor'
        else:
            author_key = 'unknown'
        
        # Remove any non-alphanumeric characters
        author_key = re.sub(r'[^a-z0-9]', '', author_key)
        
        # Add year if available
        if self.year:
            return f"{author_key}{self.year}"
        return author_key


class SRUClient:
    """
    A flexible SRU (Search/Retrieve via URL) client that can work with any SRU endpoint.
    """
    
    # Registry of record format parsers
    parsers = {}
    
    @classmethod
    def register_parser(cls, schema_name):
        """Decorator to register a parser function for a specific schema."""
        def decorator(parser_func):
            cls.parsers[schema_name] = parser_func
            return parser_func
        return decorator
    
    def __init__(self, 
                base_url: str,
                default_schema: str = None,
                version: str = "1.1",
                namespaces: Dict[str, str] = None,
                timeout: int = 30, 
                record_parser: Optional[Callable] = None,
                query_params: Dict[str, str] = None):
        """
        Initialize SRU client.
        """
        self.base_url = base_url
        self.version = version
        self.timeout = timeout
        self.default_schema = default_schema
        self.custom_parser = record_parser
        self.query_params = query_params or {}
        
        # Comprehensive set of namespaces for different record formats
        self.namespaces = {
            # SRU namespaces
            'srw': 'http://www.loc.gov/zing/srw/',
            'sd': 'http://www.loc.gov/zing/srw/diagnostic/',  # Added diagnostic namespace
            
            # Dublin Core
            'dc': 'http://purl.org/dc/elements/1.1/',
            'dcterms': 'http://purl.org/dc/terms/',
            
            # MARC
            'marc': 'http://www.loc.gov/MARC21/slim',
            'mxc': 'info:lc/xmlns/marcxchange-v2',
            
            # XML Schema
            'xsi': 'http://www.w3.org/2001/XMLSchema-instance',
            'xsd': 'http://www.w3.org/2001/XMLSchema#',
            
            # RDF and related vocabularies
            'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
            'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
            'owl': 'http://www.w3.org/2002/07/owl#',
            'skos': 'http://www.w3.org/2004/02/skos/core#',
            'foaf': 'http://xmlns.com/foaf/0.1/',
            'bibo': 'http://purl.org/ontology/bibo/',
            'schema': 'http://schema.org/',
            
            # Library specific
            'gndo': 'https://d-nb.info/standards/elementset/gnd#',
            'marcRole': 'http://id.loc.gov/vocabulary/relators/',
            'rdau': 'http://rdaregistry.info/Elements/u/',
            'isbd': 'http://iflastandards.info/ns/isbd/elements/',
            'umbel': 'http://umbel.org/umbel#',
            'gbv': 'http://purl.org/ontology/gbv/',
            
            # Thesauri and classifications
            'editeur': 'https://ns.editeur.org/thema/',
            'thesoz': 'http://lod.gesis.org/thesoz/',
            'agrovoc': 'https://aims.fao.org/aos/agrovoc/',
            'lcsh': 'https://id.loc.gov/authorities/subjects/',
            'mesh': 'http://id.nlm.nih.gov/mesh/vocab#',
            
            # Library institutions
            'dnbt': 'https://d-nb.info/standards/elementset/dnb#',
            'nsogg': 'https://purl.org/bncf/tid/',
            'ram': 'https://data.bnf.fr/ark:/12148/',
            'naf': 'https://id.loc.gov/authorities/names/',
            'embne': 'https://datos.bne.es/resource/',
            
            # Misc
            'geo': 'http://www.opengis.net/ont/geosparql#',
            'sf': 'http://www.opengis.net/ont/sf#',
            'bflc': 'http://id.loc.gov/ontologies/bflc/',
            'agrelon': 'https://d-nb.info/standards/elementset/agrelon#',
            'dcmitype': 'http://purl.org/dc/dcmitype/',
            'dbp': 'http://dbpedia.org/property/',
            'dnb_intern': 'http://dnb.de/',
            'madsrdf': 'http://www.loc.gov/mads/rdf/v1#',
            'v': 'http://www.w3.org/2006/vcard/ns#',
            'cidoc': 'http://www.cidoc-crm.org/cidoc-crm/',
            'dcatde': 'http://dcat-ap.de/def/dcatde/',
            'ebu': 'http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#',
            'wdrs': 'http://www.w3.org/2007/05/powder-s#',
            'lib': 'http://purl.org/library/',
            'mo': 'http://purl.org/ontology/mo/'
        }
        
        # Update with provided namespaces
        if namespaces:
            self.namespaces.update(namespaces)
    
    def build_query_url(self, query: str, 
                        schema: str = None,
                        max_records: int = 10,
                        start_record: int = 1) -> str:
        """
        Build SRU query URL.
        
        Args:
            query: CQL query
            schema: Record schema
            max_records: Maximum number of records to return
            start_record: Start record position
            
        Returns:
            Complete SRU query URL
        """
        schema = schema or self.default_schema
        
        # Base parameters
        params = {
            'version': self.version,
            'operation': 'searchRetrieve',
            'query': query,
            'maximumRecords': str(max_records),
            'startRecord': str(start_record)
        }
        
        # Add schema if specified
        if schema:
            params['recordSchema'] = schema
        
        # Add additional query parameters
        params.update(self.query_params)
        
        # Construct URL
        param_string = '&'.join([f"{k}={urllib.parse.quote(str(v))}" for k, v in params.items()])
        
        if '?' in self.base_url:
            return f"{self.base_url}&{param_string}"
        else:
            return f"{self.base_url}?{param_string}"
    
    def execute_query(self, query: str, 
                    schema: str = None,
                    max_records: int = 10, 
                    start_record: int = 1) -> Tuple[int, List[Dict[str, Any]]]:
        """
        Execute SRU query and return raw results.
        
        Args:
            query: CQL query
            schema: Record schema
            max_records: Maximum number of records to return
            start_record: Start record position
            
        Returns:
            Tuple of (total_records, list of raw record data)
        """
        url = self.build_query_url(query, schema, max_records, start_record)
        logger.debug(f"Querying: {url}")
        
        try:
            response = requests.get(url, timeout=self.timeout)
            response.raise_for_status()
            
            # Parse XML response
            root = ET.fromstring(response.content)
            
            # Check for diagnostics (errors)
            namespaces = {
                'srw': 'http://www.loc.gov/zing/srw/',
                'sd': 'http://www.loc.gov/zing/srw/diagnostic/'
            }
            
            # First check BNF-specific diagnostics
            bnf_diagnostics = root.findall('.//sd:diagnostic', namespaces)
            if bnf_diagnostics:
                for diag in bnf_diagnostics:
                    message_elem = diag.find('./sd:message', namespaces)
                    details_elem = diag.find('./sd:details', namespaces)
                    uri_elem = diag.find('./sd:uri', namespaces)
                    
                    # Log details if available
                    if message_elem is not None and message_elem.text:
                        logger.warning(f"SRU Diagnostic: {message_elem.text}")
                    if details_elem is not None and details_elem.text:
                        logger.warning(f"Details: {details_elem.text}")
                        # For BNF schema issues
                        if "Schéma inconnu" in details_elem.text:
                            logger.warning("The server does not support the requested schema. Try 'dublincore' instead.")
                            if url and 'recordSchema=marcxchange' in url:
                                corrected_url = url.replace('recordSchema=marcxchange', 'recordSchema=dublincore')
                                logger.info(f"Retrying with corrected URL: {corrected_url}")
                                response = requests.get(corrected_url, timeout=self.timeout)
                                response.raise_for_status()
                                root = ET.fromstring(response.content)
            
            # Check standard SRU diagnostics
            diagnostics = root.findall('.//srw:diagnostics/sd:diagnostic', namespaces)
            if diagnostics:
                for diag in diagnostics:
                    message_elem = diag.find('./sd:message', namespaces)
                    if message_elem is not None and message_elem.text:
                        logger.warning(f"SRU Diagnostic: {message_elem.text}")
                    
                    details_elem = diag.find('./sd:details', namespaces)
                    if details_elem is not None and details_elem.text:
                        logger.warning(f"Details: {details_elem.text}")
                        # For schema issues
                        if "schema" in details_elem.text.lower() and "unknown" in details_elem.text.lower():
                            logger.warning("The server does not support the requested schema. Try with a different schema.")
            
            # Get number of records
            num_records_elem = root.find('.//srw:numberOfRecords', namespaces)
            if num_records_elem is None:
                logger.warning("Could not find number of records in response")
                return 0, []
            
            try:
                num_records = int(num_records_elem.text)
                logger.debug(f"Found {num_records} records")
            except (ValueError, TypeError):
                logger.warning(f"Invalid number of records: {num_records_elem.text}")
                return 0, []
            
            if num_records == 0:
                return 0, []
            
            # Extract records
            records = []
            record_elems = root.findall('.//srw:record', namespaces)
            
            for record_elem in record_elems:
                # Get record schema
                schema_elem = record_elem.find('.//srw:recordSchema', namespaces)
                record_schema = schema_elem.text.strip() if schema_elem is not None and schema_elem.text else None
                
                # Get record data
                record_data_elem = record_elem.find('.//srw:recordData', namespaces)
                if record_data_elem is not None:
                    # Store the raw XML for the record
                    record_xml = ET.tostring(record_data_elem).decode('utf-8')
                    
                    # Get record identifier
                    record_id_elem = record_elem.find('.//srw:recordIdentifier', namespaces)
                    record_id = record_id_elem.text if record_id_elem is not None else None
                    
                    # Get record position if available
                    position_elem = record_elem.find('.//srw:recordPosition', namespaces)
                    position = position_elem.text if position_elem is not None else None
                    
                    records.append({
                        'id': record_id or position or f"record-{len(records)+1}",
                        'schema': record_schema,
                        'data': record_data_elem,
                        'raw_xml': record_xml
                    })
            
            return num_records, records
            
        except requests.RequestException as e:
            logger.error(f"Error executing query: {e}")
            return 0, []
        except ET.ParseError as e:
            logger.error(f"Error parsing XML response: {e}")
            return 0, []
        except Exception as e:
            logger.error(f"Unexpected error: {e}")
            return 0, []
    
    def search(self, query: str, 
            schema: str = None,
            max_records: int = 10, 
            start_record: int = 1) -> Tuple[int, List[BiblioRecord]]:
        """
        Search the SRU endpoint and parse records.
        
        Args:
            query: CQL query
            schema: Record schema
            max_records: Maximum number of records to return
            start_record: Start record position
            
        Returns:
            Tuple of (total_records, list of BiblioRecord objects)
        """
        total, raw_records = self.execute_query(query, schema, max_records, start_record)
        
        if not raw_records:
            return total, []
        
        records = []
        existing_ids = set()  # Track existing record IDs to avoid duplicates
        
        for raw_record in raw_records:
            try:
                # Ensure record ID is unique
                record_id = raw_record.get('id', f"record-{len(records)+1}")
                if record_id in existing_ids:
                    i = 1
                    new_id = f"{record_id}_{i}"
                    while new_id in existing_ids:
                        i += 1
                        new_id = f"{record_id}_{i}"
                    record_id = new_id
                existing_ids.add(record_id)
                raw_record['id'] = record_id
                
                # Determine the parser to use
                parser = self.custom_parser
                if not parser and raw_record['schema'] in self.parsers:
                    parser = self.parsers[raw_record['schema']]
                
                record = None
                if parser:
                    try:
                        record = parser(raw_record, self.namespaces)
                    except Exception as e:
                        logger.warning(f"Error in custom parser for record {record_id}: {e}")
                        # Fall back to generic parser
                        record = self._generic_parse(raw_record, self.namespaces)
                else:
                    # Use a generic parser as fallback
                    record = self._generic_parse(raw_record, self.namespaces)
                    
                if record:
                    # Ensure record has the correct ID and schema
                    record.id = record_id
                    record.schema = raw_record.get('schema')
                    records.append(record)
                else:
                    # Make a minimal record if all parsing failed
                    min_record = BiblioRecord(
                        id=record_id,
                        title=f"Unparseable Record {record_id}",
                        raw_data=raw_record['raw_xml'],
                        schema=raw_record.get('schema')
                    )
                    records.append(min_record)
                    logger.debug(f"Created minimal record for {record_id} due to parsing failure")
            
            except Exception as e:
                logger.error(f"Error handling record {raw_record.get('id', 'unknown')}: {e}")
                # Make a minimal record despite the error
                record_id = raw_record.get('id', f"record-{len(records)+1}")
                if record_id in existing_ids:
                    i = 1
                    new_id = f"{record_id}_{i}"
                    while new_id in existing_ids:
                        i += 1
                        new_id = f"{record_id}_{i}"
                    record_id = new_id
                existing_ids.add(record_id)
                
                try:
                    # Try to extract title from raw XML as a last resort
                    title_match = re.search(r'<dc:title[^>]*>(.*?)</dc:title>', raw_record['raw_xml'], re.DOTALL)
                    title = title_match.group(1).strip() if title_match else f"Error Record {record_id}"
                except Exception:
                    title = f"Error Record {record_id}"
                    
                min_record = BiblioRecord(
                    id=record_id,
                    title=title,
                    raw_data=raw_record['raw_xml'],
                    schema=raw_record.get('schema')
                )
                records.append(min_record)
        
        return total, records
    
    def _generic_parse(self, raw_record: Dict[str, Any], 
                    namespaces: Dict[str, str]) -> Optional[BiblioRecord]:
        """
        Generic record parser for when no specific parser is available.
        Attempts to extract basic Dublin Core or MARC data.
        
        Args:
            raw_record: Raw record data
            namespaces: XML namespaces
            
        Returns:
            BiblioRecord or None if parsing fails
        """
        record_data = raw_record['data']
        record_id = raw_record.get('id', 'unknown')
        
        # Try to find title using various possible paths
        title_paths = [
            './/dc:title', 
            './/dcterms:title',
            './/title',
            './/marc:datafield[ @tag="245"]/marc:subfield[@code="a"]',
            './/mxc:datafield[ @tag="245"]/mxc:subfield[@code="a"]',
            './/*[local-name()="title"]'
        ]
        
        title = None
        for path in title_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text and elem.text.strip():
                    title = elem.text.strip()
                    break
            except Exception:
                continue
        
        if not title:
            title = f"Untitled Record ({record_id})"
        
        # Try to find authors
        authors = []
        editors = []
        translators = []
        contributors = []
        
        # Extract creators/authors
        author_paths = [
            './/dc:creator',
            './/dcterms:creator',
            './/creator',
            './/marc:datafield[ @tag="100"]/marc:subfield[@code="a"]',
            './/mxc:datafield[ @tag="100"]/mxc:subfield[@code="a"]',
            './/marc:datafield[ @tag="700"]/marc:subfield[@code="a"]',
            './/mxc:datafield[ @tag="700"]/mxc:subfield[@code="a"]',
            './/*[local-name()="creator"]',
            './/*[local-name()="author"]'
        ]
        
        seen_names = set()  # Track seen names to avoid duplicates
        
        for path in author_paths:
            try:
                elems = record_data.findall(path, namespaces)
                for elem in elems:
                    if elem.text and elem.text.strip():
                        name = elem.text.strip()
                        
                        # Check if it's an editor
                        if re.search(r'\b(?:ed(?:itor)?|hrsg|hg)\b', name.lower(), re.IGNORECASE) or "(ed" in name.lower() or "(hg" in name.lower() or "(hg.)" in name.lower():

                            # Clean editor name by removing role designation

                            clean_name = re.sub(r'\s*[\(\[][^)]*(?:ed|hrsg|edit|hg)[^)]*[\)\]]', '', name)
                            clean_name = re.sub(r'\s*(?:ed|hrsg|edit|hg)\.?(?:\s+|$)', '', clean_name)
                            clean_name = clean_name.strip()
                            
                            if clean_name and clean_name not in seen_names:
                                editors.append(clean_name)
                                seen_names.add(clean_name)
                            continue
                        
                        # Check if it's a translator
                        if re.search(r'\b(?:trans|transl|translator|übersetz|übers)\b', name.lower(), re.IGNORECASE):
                            # Clean translator name
                            clean_name = re.sub(r'\s*[\(\[][^)]*(?:trans|übersetz)[^)]*[\)\]]', '', name)
                            clean_name = re.sub(r'\s*(?:trans|transl|translator|übersetz|übers)\.?(?:\s+|$)', '', clean_name)
                            clean_name = clean_name.strip()
                            
                            if clean_name and clean_name not in seen_names:
                                translators.append(clean_name)
                                seen_names.add(clean_name)
                            continue
                            
                        # Regular author
                        if name not in seen_names:
                            authors.append(name)
                            seen_names.add(name)
            except Exception:
                continue
        
        # Try to find year
        year = None
        date_paths = [
            './/dc:date',
            './/dcterms:date',
            './/dcterms:issued',
            './/date',
            './/marc:datafield[ @tag="260"]/marc:subfield[@code="c"]',
            './/mxc:datafield[ @tag="260"]/mxc:subfield[@code="c"]',
            './/marc:datafield[ @tag="264"]/marc:subfield[@code="c"]',
            './/mxc:datafield[ @tag="264"]/mxc:subfield[@code="c"]',
            './/*[local-name()="date"]',
            './/*[local-name()="issued"]'
        ]
        
        for path in date_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    date_text = elem.text.strip()
                    # Extract year
                    match = re.search(r'\b(1\d{3}|20\d{2})\b', date_text)
                    if match:
                        year = match.group(1)
                        break
            except Exception:
                continue
        
        # Try to find publisher
        publisher = None
        publisher_paths = [
            './/dc:publisher',
            './/dcterms:publisher',
            './/publisher',
            './/marc:datafield[ @tag="260"]/marc:subfield[@code="b"]',
            './/mxc:datafield[ @tag="260"]/mxc:subfield[@code="b"]',
            './/marc:datafield[ @tag="264"]/marc:subfield[@code="b"]',
            './/mxc:datafield[ @tag="264"]/mxc:subfield[@code="b"]',
            './/*[local-name()="publisher"]'
        ]
        
        for path in publisher_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    publisher = elem.text.strip()
                    # Clean up publisher string (remove trailing punctuation)
                    publisher = re.sub(r'[,:]$', '', publisher).strip()
                    break
            except Exception:
                continue
        
        # Try to find place of publication
        place_of_publication = None
        place_paths = [
            './/marc:datafield[ @tag="260"]/marc:subfield[@code="a"]',
            './/mxc:datafield[ @tag="260"]/mxc:subfield[@code="a"]',
            './/marc:datafield[ @tag="264"]/marc:subfield[@code="a"]',
            './/mxc:datafield[ @tag="264"]/mxc:subfield[@code="a"]'
        ]
        
        for path in place_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    place_of_publication = elem.text.strip()
                    # Clean up place (remove trailing punctuation)
                    place_of_publication = re.sub(r'[,:]$', '', place_of_publication).strip()
                    break
            except Exception:
                continue
        
        # Try to find ISBN
        isbn = None
        isbn_paths = [
            './/bibo:isbn13',
            './/bibo:isbn10',
            './/bibo:isbn',
            './/bibo:gtin14',
            './/dc:identifier[contains(text(), "ISBN")]',
            './/marc:datafield[ @tag="020"]/marc:subfield[@code="a"]',
            './/mxc:datafield[ @tag="020"]/mxc:subfield[@code="a"]',
            './/*[local-name()="identifier" and contains(text(), "ISBN")]'
        ]
        
        for path in isbn_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    isbn_text = elem.text.strip()
                    # Extract ISBN
                    match = re.search(r'(?:ISBN[:\s]*)?(\d[\d\-X]+)', isbn_text)
                    if match:
                        isbn = match.group(1)
                        break
                    else:
                        isbn = isbn_text
                        break
            except Exception:
                continue
        
        # Try to find ISSN
        issn = None
        issn_paths = [
            './/bibo:issn',
            './/dc:identifier[contains(text(), "ISSN")]',
            './/marc:datafield[ @tag="022"]/marc:subfield[@code="a"]',
            './/mxc:datafield[ @tag="022"]/mxc:subfield[@code="a"]',
            './/*[local-name()="identifier" and contains(text(), "ISSN")]'
        ]
        
        for path in issn_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    issn_text = elem.text.strip()
                    # Extract ISSN
                    match = re.search(r'(?:ISSN[:\s]*)?(\d{4}-\d{3}[\dX])', issn_text)
                    if match:
                        issn = match.group(1)
                        break
                    else:
                        issn = issn_text
                        break
            except Exception:
                continue
        
        # Try to find journal title (for articles)
        journal_title = None
        journal_paths = [
            './/marc:datafield[ @tag="773"]/marc:subfield[@code="t"]',
            './/mxc:datafield[ @tag="773"]/mxc:subfield[@code="t"]',
            './/marc:datafield[ @tag="773"]/marc:subfield[@code="p"]',
            './/mxc:datafield[ @tag="773"]/mxc:subfield[@code="p"]'
        ]
        
        for path in journal_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    journal_title = elem.text.strip()
                    break
            except Exception:
                continue
        
        # Try to find volume and issue
        volume = None
        issue = None
        
        # Volume
        volume_paths = [
            './/marc:datafield[ @tag="773"]/marc:subfield[@code="v"]',
            './/mxc:datafield[ @tag="773"]/mxc:subfield[@code="v"]'
        ]
        
        for path in volume_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    volume = elem.text.strip()
                    break
            except Exception:
                continue
        
        # Issue
        issue_paths = [
            './/marc:datafield[ @tag="773"]/marc:subfield[@code="l"]',
            './/mxc:datafield[ @tag="773"]/mxc:subfield[@code="l"]'
        ]
        
        for path in issue_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    issue = elem.text.strip()
                    break
            except Exception:
                continue
        
        # Try to find page range
        pages = None
        pages_paths = [
            './/marc:datafield[ @tag="773"]/marc:subfield[@code="g"]',
            './/mxc:datafield[ @tag="773"]/mxc:subfield[@code="g"]'
        ]
        
        for path in pages_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    # Try to extract page range from various formats
                    page_text = elem.text.strip()
                    page_match = re.search(r'p\.?\s*(\d+(?:-\d+)?)', page_text, re.IGNORECASE)
                    if page_match:
                        pages = page_match.group(1)
                    else:
                        # Just use raw text if no pattern matched
                        pages = page_text
                    break
            except Exception:
                continue
        
        # Try to find extent (number of pages for books)
        extent = None
        extent_paths = [
            './/marc:datafield[ @tag="300"]/marc:subfield[@code="a"]',
            './/mxc:datafield[ @tag="300"]/mxc:subfield[@code="a"]'
        ]
        
        for path in extent_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    extent = elem.text.strip()
                    break
            except Exception:
                continue
        
        # Try to find DOI
        doi = None
        doi_paths = [
            './/dc:identifier[contains(text(), "doi")]',
            './/marc:datafield[ @tag="024"][@ind1="7"]/marc:subfield[@code="a"][../marc:subfield[@code="2"]="doi"]',
            './/mxc:datafield[ @tag="024"][@ind1="7"]/mxc:subfield[@code="a"][../mxc:subfield[@code="2"]="doi"]'
        ]
        
        # Fixed implementation for finding DOI that doesn't use getparent()
        for path in ['.//marc:datafield[ @tag="024"][@ind1="7"]', './/mxc:datafield[ @tag="024"][@ind1="7"]']:
            try:
                fields = record_data.findall(path, namespaces)
                for field in fields:
                    type_subfield = field.find('./marc:subfield[ @code="2"]', namespaces) or field.find('./mxc:subfield[ @code="2"]', namespaces)
                    value_subfield = field.find('./marc:subfield[ @code="a"]', namespaces) or field.find('./mxc:subfield[ @code="a"]', namespaces)
                    
                    if (type_subfield is not None and type_subfield.text 
                            and type_subfield.text.strip().lower() == "doi" 
                            and value_subfield is not None and value_subfield.text):
                        doi = value_subfield.text.strip()
                        break
            except Exception:
                continue
        
        # Try to find document type
        document_type = None
        leader = None
        
        try:
            leader_elem = record_data.find('.//marc:leader', namespaces)
            if not leader_elem:
                leader_elem = record_data.find('.//mxc:leader', namespaces)
            if leader_elem is not None and leader_elem.text:
                leader = leader_elem.text
        except Exception:
            pass
        
        if leader:
            # Position 6 and 7 in MARC leader indicate record type and bibliographic level
            if len(leader) >= 8:
                record_type = leader[6]
                biblio_level = leader[7]
                
                if record_type == 'a' and biblio_level == 's':
                    document_type = 'Journal'
                elif record_type == 'a' and biblio_level == 'm':
                    document_type = 'Book'
                elif record_type == 'a' and biblio_level == 'a':
                    document_type = 'Journal Article'
                elif record_type == 'a' and biblio_level == 'c':
                    document_type = 'Book Chapter'
                elif record_type == 'e':
                    document_type = 'Map'
                elif record_type == 'g':
                    document_type = 'Video'
                elif record_type == 'j':
                    document_type = 'Music'
                elif record_type == 'k':
                    document_type = 'Image'
                elif record_type == 'm':
                    document_type = 'Computer File'
        
        # Infer document type from other clues if not found in leader
        if not document_type:
            if journal_title and (pages or volume or issue):
                document_type = 'Journal Article'
            elif issn:
                document_type = 'Journal'
            elif isbn:
                document_type = 'Book'
        
        # Try to find URLs
        urls = []
        url_paths = [
            './/foaf:primaryTopic',
            './/umbel:isLike',
            './/dc:identifier[contains(text(), "http")]',
            './/marc:datafield[ @tag="856"]/marc:subfield[@code="u"]',
            './/mxc:datafield[ @tag="856"]/mxc:subfield[@code="u"]'
        ]
        
        for path in url_paths:
            try:
                elems = record_data.findall(path, namespaces)
                for elem in elems:
                    # Check for resource attribute first (RDF style)
                    resource = elem.get('{'+namespaces.get('rdf', '')+'}resource')
                    if resource and resource.startswith('http'):
                        urls.append(resource)
                    # Check for text content
                    elif elem.text and elem.text.strip().startswith('http'):
                        urls.append(elem.text.strip())
            except Exception:
                continue
        
        # Try to find subjects
        subjects = []
        subject_paths = [
            './/dc:subject',
            './/dcterms:subject',
            './/marc:datafield[ @tag="650"]/marc:subfield[@code="a"]',
            './/mxc:datafield[ @tag="650"]/mxc:subfield[@code="a"]',
            './/marc:datafield[ @tag="651"]/marc:subfield[@code="a"]',
            './/mxc:datafield[ @tag="651"]/mxc:subfield[@code="a"]',
            './/marc:datafield[ @tag="653"]/marc:subfield[@code="a"]',
            './/mxc:datafield[ @tag="653"]/mxc:subfield[@code="a"]'
        ]
        
        for path in subject_paths:
            try:
                elems = record_data.findall(path, namespaces)
                for elem in elems:
                    if elem.text and elem.text.strip():
                        subjects.append(elem.text.strip())
            except Exception:
                continue
        
        # Try to find abstract/description
        abstract = None
        abstract_paths = [
            './/dc:description',
            './/dcterms:abstract',
            './/marc:datafield[ @tag="520"]/marc:subfield[@code="a"]',
            './/mxc:datafield[ @tag="520"]/mxc:subfield[@code="a"]'
        ]
        
        for path in abstract_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    abstract = elem.text.strip()
                    break
            except Exception:
                continue
        
        # Try to find language
        language = None
        language_paths = [
            './/dc:language',
            './/dcterms:language',
            './/marc:datafield[ @tag="041"]/marc:subfield[@code="a"]',
            './/mxc:datafield[ @tag="041"]/mxc:subfield[@code="a"]'
        ]
        
        for path in language_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    language = elem.text.strip()
                    break
            except Exception:
                continue
        
        # Try to find series
        series = None
        series_paths = [
            './/marc:datafield[ @tag="490"]/marc:subfield[@code="a"]',
            './/mxc:datafield[ @tag="490"]/mxc:subfield[@code="a"]',
            './/marc:datafield[ @tag="830"]/marc:subfield[@code="a"]',
            './/mxc:datafield[ @tag="830"]/mxc:subfield[@code="a"]'
        ]
        
        for path in series_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    series = elem.text.strip()
                    break
            except Exception:
                continue
        
        # Try to find edition
        edition = None
        edition_paths = [
            './/marc:datafield[ @tag="250"]/marc:subfield[@code="a"]',
            './/mxc:datafield[ @tag="250"]/mxc:subfield[@code="a"]'
        ]
        
        for path in edition_paths:
            try:
                elem = record_data.find(path, namespaces)
                if elem is not None and elem.text:
                    edition = elem.text.strip()
                    break
            except Exception:
                continue
        
        # Create BiblioRecord with all extracted fields
        return BiblioRecord(
            id=record_id,
            title=title,
            authors=authors,
            editors=editors,
            translators=translators,
            contributors=contributors,
            year=year,
            publisher_name=publisher,
            place_of_publication=place_of_publication,
            isbn=isbn,
            issn=issn,
            urls=urls,
            abstract=abstract,
            language=language,
            format=document_type,  # Use detected document_type as format
            subjects=subjects,
            series=series,
            extent=extent,
            edition=edition,
            journal_title=journal_title,
            volume=volume,
            issue=issue,
            pages=pages,
            doi=doi,
            document_type=document_type,
            raw_data=raw_record['raw_xml'],
            schema=raw_record.get('schema')
        )
    
    def _extract_text(self, elem: ET.Element, xpath_list: List[str], 
                     namespaces: Dict[str, str]) -> Optional[str]:
        """Extract text using a list of XPath expressions, trying each until one succeeds."""
        for xpath in xpath_list:
            result = elem.find(xpath, namespaces)
            if result is not None and result.text and result.text.strip():
                return result.text.strip()
        return None
    
    def _find_elements(self, elem: ET.Element, xpath_list: List[str], 
                       namespaces: Dict[str, str]) -> List[ET.Element]:
        """Find elements using a list of XPath expressions, trying each until one succeeds."""
        for xpath in xpath_list:
            results = elem.findall(xpath, namespaces)
            if results:
                return results
        return []


# Register parser for Dublin Core format
@SRUClient.register_parser('info:srw/schema/1/dc-v1.1')
@SRUClient.register_parser('dc')
@SRUClient.register_parser('dublincore')
def parse_dublin_core(raw_record, namespaces):
    """Parse Dublin Core format records."""
    data = raw_record['data']
    record_id = raw_record.get('id', 'unknown')
    
    # Add Dublin Core namespace if not present
    ns = namespaces.copy()
    if 'dc' not in ns:
        ns['dc'] = 'http://purl.org/dc/elements/1.1/'
    if 'dcterms' not in ns:
        ns['dcterms'] = 'http://purl.org/dc/terms/'
    
    # Find title
    title_elem = data.find('.//dc:title', ns)
    title = title_elem.text.strip() if title_elem is not None and title_elem.text else "Untitled"
    
    # Find authors and contributors with proper separation
    authors = []
    editors = []
    translators = []
    contributors = []
    
    # Keep track of seen names to avoid duplicates
    seen_names = set()
    
    # Process creators (authors)
    creator_elems = data.findall('.//dc:creator', ns)
    for elem in creator_elems:
        if elem.text and elem.text.strip():
            name = elem.text.strip()
            
            # Check if it's an editor
            if re.search(r'\b(?:ed(?:itor)?|hrsg|hg|edit\.)\b', name.lower(), re.IGNORECASE) or "(ed" in name.lower():
                # Clean editor name by removing role designation
                clean_name = re.sub(r'\s*[\(\[][^)]*(?:ed|hrsg|edit|hg)[^)]*[\)\]]', '', name)
                clean_name = re.sub(r'\s*(?:ed|hrsg|edit|hg)\.?(?:\s+|$)', '', clean_name)
                clean_name = clean_name.strip()
                
                if clean_name and clean_name not in seen_names:
                    editors.append(clean_name)
                    seen_names.add(clean_name)
                continue
                
            # Check if it's a translator
            if re.search(r'\b(?:trans|transl|translator|übersetz|übers)\b', name.lower(), re.IGNORECASE):
                # Clean translator name
                clean_name = re.sub(r'\s*[\(\[][^)]*(?:trans|übersetz)[^)]*[\)\]]', '', name)
                clean_name = re.sub(r'\s*(?:trans|transl|translator|übersetz|übers)\.?(?:\s+|$)', '', clean_name)
                clean_name = clean_name.strip()
                
                if clean_name and clean_name not in seen_names:
                    translators.append(clean_name)
                    seen_names.add(clean_name)
                continue
                
            # Regular author (not an editor or translator)
            if name not in seen_names:
                authors.append(name)
                seen_names.add(name)
    
    # Process contributors (could be editors for book chapters)
    contributor_elems = data.findall('.//dc:contributor', ns)
    for elem in contributor_elems:
        if elem.text and elem.text.strip():
            contributor = elem.text.strip()
            
            # Check if it's an editor
            if re.search(r'\b(?:ed(?:itor)?|hrsg|hg)\b', contributor.lower(), re.IGNORECASE) or "(ed" in contributor.lower():
                # Clean editor name
                clean_name = re.sub(r'\s*[\(\[][^)]*(?:ed|hrsg|edit|hg)[^)]*[\)\]]', '', contributor)
                clean_name = re.sub(r'\s*(?:ed|hrsg|edit|hg)\.?(?:\s+|$)', '', clean_name)
                clean_name = clean_name.strip()
                
                if clean_name and clean_name not in seen_names:
                    editors.append(clean_name)
                    seen_names.add(clean_name)
                continue
                
            # Check if it's a translator
            if re.search(r'\b(?:trans|transl|translator|übersetz|übers)\b', contributor.lower(), re.IGNORECASE):
                # Clean translator name
                clean_name = re.sub(r'\s*[\(\[][^)]*(?:trans|übersetz)[^)]*[\)\]]', '', contributor)
                clean_name = re.sub(r'\s*(?:trans|transl|translator|übersetz|übers)\.?(?:\s+|$)', '', clean_name)
                clean_name = clean_name.strip()
                
                if clean_name and clean_name not in seen_names:
                    translators.append(clean_name)
                    seen_names.add(clean_name)
                continue
                
            # Other contributors with no specific role identified
            if contributor not in seen_names:
                contributors.append({"name": contributor, "role": "contributor"})
                seen_names.add(contributor)
    
    # Find dates
    date_elem = data.find('.//dc:date', ns)
    year = None
    if date_elem is not None and date_elem.text:
        date_text = date_elem.text.strip()
        # Extract year
        match = re.search(r'\b(1\d{3}|20\d{2})\b', date_text)
        if match:
            year = match.group(1)
    
    # Find publisher
    publisher_elem = data.find('.//dc:publisher', ns)
    publisher = publisher_elem.text.strip() if publisher_elem is not None and publisher_elem.text else None
    
    # Find identifiers (ISBN, ISSN, DOI)
    isbn = None
    issn = None
    doi = None
    
    identifier_elems = data.findall('.//dc:identifier', ns)
    for elem in identifier_elems:
        if not elem.text:
            continue
            
        id_text = elem.text.strip().lower()
        
        # Extract ISBN
        if 'isbn' in id_text:
            match = re.search(r'(?:isbn[:\s]*)?(\d[\d\-X]+)', id_text)
            if match:
                isbn = match.group(1)
                
        # Extract ISSN
        elif 'issn' in id_text:
            match = re.search(r'(?:issn[:\s]*)?(\d{4}-\d{3}[\dX])', id_text)
            if match:
                issn = match.group(1)
                
        # Extract DOI
        elif 'doi' in id_text or 'doi.org' in id_text:
            # DOI format is flexible, but commonly 10.NNNN/rest
            match = re.search(r'(?:doi[:\s]*)?(?:https?://doi.org/)?(\d+\.\d+/[^\s]+)', id_text)
            if match:
                doi = match.group(1)
    
    # Find subjects
    subjects = []
    subject_elems = data.findall('.//dc:subject', ns)
    for elem in subject_elems:
        if elem.text and elem.text.strip():
            subjects.append(elem.text.strip())
    
    # Find description (abstract)
    abstract = None
    description_elem = data.find('.//dc:description', ns)
    if description_elem is not None and description_elem.text:
        abstract = description_elem.text.strip()
    
    # Find language
    language = None
    language_elem = data.find('.//dc:language', ns)
    if language_elem is not None and language_elem.text:
        language = language_elem.text.strip()
    
    # Find format
    format_str = None
    format_elem = data.find('.//dc:format', ns)
    if format_elem is not None and format_elem.text:
        format_str = format_elem.text.strip()
    
    # Find extent (pages, etc.)
    extent = None
    extent_elem = data.find('.//dcterms:extent', ns)
    if extent_elem is not None and extent_elem.text:
        extent = extent_elem.text.strip()
        
        # Try to extract page range
        pages = None
        page_match = re.search(r'(\d+)(?:\s*[-–]\s*(\d+))?\s*p', extent)
        if page_match:
            if page_match.group(2):  # Range of pages
                pages = f"{page_match.group(1)}-{page_match.group(2)}"
            else:  # Single page count
                pages = page_match.group(1)
    
    # Find source (could contain book or journal info)
    source_elem = data.find('.//dc:source', ns)
    source = source_elem.text.strip() if source_elem is not None and source_elem.text else None
    
    # Initialize journal/book related variables
    journal_title = None
    volume = None
    issue = None
    series = None
    pages = None
    
    # Process source to extract journal info if available
    if source:
        # Check for journal pattern like "Journal Name, Vol. X, No. Y, pp. Z-W"
        journal_match = re.search(r'([^,]+),\s*(?:Vol(?:ume)?\.?\s*(\d+))?,?\s*(?:No\.?\s*(\d+))?,?\s*(?:pp\.?\s*(\d+(?:-\d+)?))?', source)
        if journal_match:
            journal_title = journal_match.group(1).strip()
            volume = journal_match.group(2)
            issue = journal_match.group(3)
            if journal_match.group(4):
                pages = journal_match.group(4)
                
        # If not journal, might be a book chapter or series
        elif 'in:' in source.lower() or 'in ' in source.lower():
            book_match = re.search(r'(?:in:?|In:?)\s*([^,]+)', source)
            if book_match:
                series = book_match.group(1).strip()
    
    # Determine document type from available info
    document_type = None
    if journal_title and (volume or issue):
        document_type = "Journal Article"
    elif series:
        document_type = "Book Chapter"
    elif 'book' in format_str.lower() if format_str else False:
        document_type = "Book"
    
    # Create and return BiblioRecord with all extracted info
    return BiblioRecord(
        id=record_id,
        title=title,
        authors=authors,
        editors=editors,
        translators=translators,
        contributors=contributors,
        year=year,
        publisher_name=publisher,
        place_of_publication=None,  # Dublin Core doesn't typically have this
        isbn=isbn,
        issn=issn,
        urls=[],  # URLs should be extracted separately
        abstract=abstract,
        language=language,
        format=format_str,
        subjects=subjects,
        series=series,  # Use series field for book title in case of book chapter
        extent=extent,
        edition=None,  # Edition info not typically in basic Dublin Core
        
        # Journal article fields
        journal_title=journal_title,
        volume=volume,
        issue=issue,
        pages=pages,
        doi=doi,
        document_type=document_type,
        schema=raw_record.get('schema')
    )

# Register parser for MARCXML format
@SRUClient.register_parser('marcxml')
@SRUClient.register_parser('info:srw/schema/1/marcxml-v1.1')
@SRUClient.register_parser('MARC21-xml')
def parse_marcxml(raw_record, namespaces):
    """Parse MARCXML format records."""
    data = raw_record['data']
    record_id = raw_record.get('id', 'unknown')
    
    # Add MARC namespace if not present
    ns = namespaces.copy()
    if 'marc' not in ns:
        ns['marc'] = 'http://www.loc.gov/MARC21/slim'
    if 'mxc' not in ns:
        ns['mxc'] = 'info:lc/xmlns/marcxchange-v2'
    
    # Find record element (which might be nested differently depending on the source)
    record = data
    
    # Check if data element is already a record element
    marc_record_tags = [
        '{http://www.loc.gov/MARC21/slim}record',
        '{info:lc/xmlns/marcxchange-v2}record'
    ]
    
    if not any(record.tag == tag for tag in marc_record_tags):
        # Try to find record element if data is not already a record
        for prefix in ['marc', 'mxc']:
            record_elem = data.find(f'.//{prefix}:record', ns)
            if record_elem is not None:
                record = record_elem
                break
                
        # If still not found, try with local-name
        if record == data:
            record_elem = data.find('.//*[local-name()="record"]')
            if record_elem is not None:
                record = record_elem
    
    # Helper function to find datafields
    def find_datafields(tag, code):
        fields = []
        for prefix in ['marc', 'mxc']:
            elems = record.findall(f'.//{prefix}:datafield[ @tag="{tag}"]/{prefix}:subfield[@code="{code}"]', ns)
            if elems:
                fields.extend([f.text.strip() for f in elems if f.text and f.text.strip()])
        return fields
    
    # Find title (MARC field 245 subfield a)
    title = "Untitled"
    title_fields = find_datafields("245", "a")
    if title_fields:
        title = title_fields[0]
        # Some titles end with / or : or other punctuation
        title = re.sub(r'[/:]$', '', title).strip()
    
    # Find subtitle if present (245 subfield b)
    subtitle_fields = find_datafields("245", "b")
    if subtitle_fields:
        title += ": " + subtitle_fields[0].strip()
    
    # Find authors, editors, translators, and other contributors with proper separation
    authors = []
    editors = []
    translators = []
    contributors = []
    
    # Keep track of seen names to avoid duplicates
    seen_names = set()
    
    # Creator (100)
    for prefix in ['marc', 'mxc']:
        creator_fields = record.findall(f'.//{prefix}:datafield[ @tag="100"]', ns)
        for field in creator_fields:
            name_subfield = field.find(f'./{prefix}:subfield[ @code="a"]', ns)
            if name_subfield is not None and name_subfield.text:
                name = name_subfield.text.strip()
                # Check for role in subfield e
                role_subfield = field.find(f'./{prefix}:subfield[ @code="e"]', ns)
                role = role_subfield.text.strip().lower() if role_subfield is not None and role_subfield.text else ''
                
                if role:
                    if any(r in role for r in ['edit', 'hrsg', 'hg']):
                        if name not in seen_names:
                            editors.append(name)
                            seen_names.add(name)
                    elif any(r in role for r in ['transl', 'übers']):
                        if name not in seen_names:
                            translators.append(name)
                            seen_names.add(name)
                    else:
                        # Other contributor role
                        if name not in seen_names:
                            contributors.append({"name": name, "role": role})
                            seen_names.add(name)
                else:
                    # No specific role, assume author
                    if name not in seen_names:
                        authors.append(name)
                        seen_names.add(name)
    
    # Contributors (700)
    for prefix in ['marc', 'mxc']:
        contributor_fields = record.findall(f'.//{prefix}:datafield[ @tag="700"]', ns)
        for field in contributor_fields:
            name_subfield = field.find(f'./{prefix}:subfield[ @code="a"]', ns)
            if name_subfield is not None and name_subfield.text:
                name = name_subfield.text.strip()
                # Check for role in subfield e
                role_subfield = field.find(f'./{prefix}:subfield[ @code="e"]', ns)
                role = role_subfield.text.strip().lower() if role_subfield is not None and role_subfield.text else ''
                
                if role:
                    if any(r in role for r in ['edit', 'hrsg', 'hg']):
                        if name not in seen_names:
                            editors.append(name)
                            seen_names.add(name)
                    elif any(r in role for r in ['transl', 'übers']):
                        if name not in seen_names:
                            translators.append(name)
                            seen_names.add(name)
                    else:
                        # Other contributor role
                        if name not in seen_names:
                            contributors.append({"name": name, "role": role})
                            seen_names.add(name)
                else:
                    # No specific role, assume author/contributor
                    if name not in seen_names:
                        authors.append(name)
                        seen_names.add(name)
    
    # If no authors but we have editors, use editors
    if not authors and editors:
        # We'll leave editors in editors field, they'll be displayed as "edited by"
        pass
    
    # Find year (MARC field 260/264 subfield c)
    year = None
    for tag in ['260', '264']:
        date_fields = find_datafields(tag, "c")
        if date_fields:
            date_text = date_fields[0]
            # Extract year
            match = re.search(r'\b(1\d{3}|20\d{2})\b', date_text)
            if match:
                year = match.group(1)
                break
    
    # Find publisher (MARC field 260/264 subfield b)
    publisher = None
    for tag in ['260', '264']:
        publisher_fields = find_datafields(tag, "b")
        if publisher_fields:
            publisher = publisher_fields[0]
            # Some publishers end with , or : or other punctuation
            publisher = re.sub(r'[,:]$', '', publisher).strip()
            break
    
    # Find place of publication (MARC field 260/264 subfield a)
    place = None
    for tag in ['260', '264']:
        place_fields = find_datafields(tag, "a")
        if place_fields:
            place = place_fields[0]
            # Some places end with : or other punctuation
            place = re.sub(r':$', '', place).strip()
            break
    
    # Find ISBN (MARC field 020 subfield a)
    isbn = None
    isbn_fields = find_datafields("020", "a")
    if isbn_fields:
        isbn_text = isbn_fields[0]
        # Extract just the ISBN part
        match = re.search(r'(\d[\d\-X]+)', isbn_text)
        if match:
            isbn = match.group(1)
    
    # Find ISSN (MARC field 022 subfield a)
    issn = None
    issn_fields = find_datafields("022", "a")
    if issn_fields:
        issn = issn_fields[0]
    
    # Find DOI (MARC field 024 subfield a, with indicator 7 and subfield 2 = doi)
    doi = None
    for prefix in ['marc', 'mxc']:
        doi_fields = record.findall(f'.//{prefix}:datafield[ @tag="024" and @ind1="7"]', ns)
        for field in doi_fields:
            subfield_2 = field.find(f'./{prefix}:subfield[ @code="2"]', ns)
            subfield_a = field.find(f'./{prefix}:subfield[ @code="a"]', ns)
            
            if (subfield_2 is not None and subfield_2.text and 
                subfield_2.text.strip().lower() == "doi" and
                subfield_a is not None and subfield_a.text):
                doi = subfield_a.text.strip()
                break
    
    # Find subjects (MARC fields 650, 651)
    subjects = []
    for tag in ['650', '651']:
        subject_fields = find_datafields(tag, "a")
        subjects.extend(subject_fields)
    
    # Find language (MARC field 041 subfield a or 008 positions 35-37)
    language = None
    language_fields = find_datafields("041", "a")
    if language_fields:
        language = language_fields[0]
    
    # Find series (MARC field 490 or 830)
    series = None
    series_fields = find_datafields("490", "a") or find_datafields("830", "a")
    if series_fields:
        series = series_fields[0]
    
    # Find extent/pagination (MARC field 300 subfield a)
    extent = None
    extent_fields = find_datafields("300", "a")
    if extent_fields:
        extent = extent_fields[0]
    
    # Extract page information from extent
    pages = None
    if extent:
        page_match = re.search(r'(\d+)(?:\s*[-–]\s*(\d+))?\s*p', extent)
        if page_match:
            if page_match.group(2):  # Range of pages
                pages = f"{page_match.group(1)}-{page_match.group(2)}"
            else:  # Single page count
                pages = page_match.group(1)
    
    # Find edition (MARC field 250 subfield a)
    edition = None
    edition_fields = find_datafields("250", "a")
    if edition_fields:
        edition = edition_fields[0]
    
    # Find URLs (MARC field 856 subfield u)
    urls = find_datafields("856", "u")
    
    # Find if this is a journal article or book chapter
    # Host item entry is in MARC field 773
    journal_title = None
    volume = None
    issue = None
    
    for prefix in ['marc', 'mxc']:
        host_item_fields = record.findall(f'.//{prefix}:datafield[ @tag="773"]', ns)
        for field in host_item_fields:
            # Title of host item (journal or book title)
            title_subfield = field.find(f'./{prefix}:subfield[ @code="t"]', ns)
            if title_subfield is not None and title_subfield.text:
                host_title = title_subfield.text.strip()
                
                # Check for journal by looking for volume info
                g_subfield = field.find(f'./{prefix}:subfield[ @code="g"]', ns)
                if g_subfield is not None and g_subfield.text:
                    vol_text = g_subfield.text.strip()
                    # Check if this looks like a journal reference
                    if re.search(r'vol|issue|number|no\.|band', vol_text.lower()):
                        journal_title = host_title
                        
                        # Extract volume/issue from text like "vol. 10, no. 3, p. 45-67"
                        vol_match = re.search(r'vol(?:ume)?\.?\s*(\d+)', vol_text, re.IGNORECASE)
                        if vol_match:
                            volume = vol_match.group(1)
                        
                        issue_match = re.search(r'(?:no|issue|num)\.?\s*(\d+)', vol_text, re.IGNORECASE)
                        if issue_match:
                            issue = issue_match.group(1)
                        
                        # Extract page range
                        page_match = re.search(r'p(?:age)?s?\.?\s*(\d+)(?:\s*[-–]\s*(\d+))?', vol_text, re.IGNORECASE)
                        if page_match:
                            if page_match.group(2):  # Range
                                pages = f"{page_match.group(1)}-{page_match.group(2)}"
                            else:  # Single page
                                pages = page_match.group(1)
                    else:
                        # Likely a book chapter - use series field
                        series = host_title
    
    # Determine document type
    document_type = None
    if journal_title:
        document_type = "Journal Article"
    elif series and not journal_title:
        document_type = "Book Chapter"
    else:
        # Leader position 6 often indicates material type
        for prefix in ['marc', 'mxc']:
            leader_elem = record.find(f'.//{prefix}:leader', ns)
            if leader_elem is not None and leader_elem.text:
                material_type = leader_elem.text[6] if len(leader_elem.text) > 6 else None
                if material_type == 'a':  # Language material
                    document_type = "Book"
                elif material_type == 'e':  # Printed music
                    document_type = "Score"
                elif material_type == 'g':  # Projected medium
                    document_type = "Visual Material"
                elif material_type == 'i':  # Nonmusical sound recording
                    document_type = "Audio Recording"
                elif material_type == 'j':  # Musical sound recording
                    document_type = "Music Recording"
                elif material_type == 'm':  # Computer file
                    document_type = "Electronic Resource"
                break
    
    # Create BiblioRecord
    return BiblioRecord(
        id=record_id,
        title=title,
        authors=authors,
        editors=editors,
        translators=translators,
        contributors=contributors,
        year=year,
        publisher_name=publisher,
        place_of_publication=place,
        isbn=isbn,
        issn=issn,
        urls=urls,
        abstract=None,  # MARC doesn't typically have abstracts in basic records
        language=language,
        format=document_type,  # Use determined document type
        subjects=subjects,
        series=series,  # For book chapters, store the book title in series field
        extent=extent,
        edition=edition,
        
        # Journal article fields
        journal_title=journal_title,
        volume=volume,
        issue=issue,
        pages=pages,
        doi=doi,
        document_type=document_type,
        schema=raw_record.get('schema')
    )

# Register parser for RDF/XML format
@SRUClient.register_parser('RDFxml')
def parse_rdfxml(raw_record, namespaces):
    """Parse RDF/XML format records (like those from DNB)."""
    data = raw_record['data']
    record_id = raw_record.get('id', 'unknown')
    
    # Debug log to see what we're parsing
    logger.debug(f"Parsing RDFxml record {record_id}")
    
    # Complete set of namespaces for RDF records
    ns = {
        'rdf': 'http://www.w3.org/1999/02/22-rdf-syntax-ns#',
        'dc': 'http://purl.org/dc/elements/1.1/',
        'dcterms': 'http://purl.org/dc/terms/',
        'bibo': 'http://purl.org/ontology/bibo/',
        'gndo': 'https://d-nb.info/standards/elementset/gnd#',
        'marcRole': 'http://id.loc.gov/vocabulary/relators/',
        'rdau': 'http://rdaregistry.info/Elements/u/',
        'schema': 'http://schema.org/',
        'foaf': 'http://xmlns.com/foaf/0.1/',
        'owl': 'http://www.w3.org/2002/07/owl#',
        'skos': 'http://www.w3.org/2004/02/skos/core#',
        'xsd': 'http://www.w3.org/2001/XMLSchema#',
        'rdfs': 'http://www.w3.org/2000/01/rdf-schema#',
        'umbel': 'http://umbel.org/umbel#',
        'isbd': 'http://iflastandards.info/ns/isbd/elements/'
    }
    
    # Update with any additional namespaces from the client
    if namespaces:
        for k, v in namespaces.items():
            if k not in ns:
                ns[k] = v
    
    # Log raw data for verbose debugging
    logger.debug(f"Raw record data for {record_id}:")
    logger.debug(raw_record['raw_xml'][:500] + "..." if len(raw_record['raw_xml']) > 500 else raw_record['raw_xml'])
    
    # Find description element
    desc = data.find('.//rdf:Description', ns)
    if desc is None:
        logger.warning(f"No RDF:Description found in record {record_id}")
        return None
    
    # Find title - direct approach 
    title = "Untitled"
    title_elem = desc.find('./dc:title', ns)
    if title_elem is not None and title_elem.text:
        title = title_elem.text.strip()
        logger.debug(f"Found title: {title}")
    else:
        logger.warning(f"No title found for record {record_id}")
    
    # Find alternative titles
    alt_title_elem = desc.find('./dcterms:alternative', ns)
    if alt_title_elem is not None and alt_title_elem.text:
        alt_title = alt_title_elem.text.strip()
        if alt_title and ":" not in title:
            title = f"{title}: {alt_title}"
            logger.debug(f"Added alternative title, full title is now: {title}")
    
    # Initialize contributor lists
    authors = []
    editors = []
    translators = []
    contributors = []
    
    seen_names = set()  # Track seen names to avoid duplicates
    
    # Function to clean author name (remove duplicate entries, trailing commas, etc.)
    def clean_author_name(name):
        # Remove trailing commas and whitespace
        name = re.sub(r',\s*$', '', name.strip())
        return name
    
    # Helper function to detect and process contributor roles
    def process_name_with_role(name, seen_names_set):
        """
        Process a name string, detect role patterns, clean the name,
        and return the cleaned name and detected role.
        
        Args:
            name: The raw name string potentially containing role information
            seen_names_set: Set to track seen names (to avoid duplicates)
            
        Returns:
            tuple: (cleaned_name, role, is_duplicate)
        """
        if name is None:
            return None, None, True
            
        name = name.strip()
        if not name:
            return None, None, True
        
        # Detect editor patterns (German and English)
        editor_patterns = [
            r'\([Hh]g\.?\)', r'\([Hh]rsg\.?\)', r'\([Ee]d\.?\)', r'\([Ee]ditor[s]?\)',
            r'\b[Hh]g\.', r'\b[Hh]rsg\.', r'\b[Ee]d\.', r'\b[Ee]ditor[s]?\b',
            r'[,\s]+[Hh]g\.', r'[,\s]+[Hh]rsg\.', r'[,\s]+[Ee]d\.', r'[,\s]+[Ee]ditor[s]?'
        ]
        
        # Detect translator patterns (German and English)
        translator_patterns = [
            r'\([Üü]bers\.?\)', r'\([Tt]rans\.?\)', r'\([Tt]ranslator[s]?\)',
            r'\b[Üü]bers\.', r'\b[Tt]rans\.', r'\b[Tt]ranslator[s]?\b',
            r'[,\s]+[Üü]bers\.', r'[,\s]+[Tt]rans\.', r'[,\s]+[Tt]ranslator[s]?'
        ]
        
        # Check for editor patterns
        is_editor = any(re.search(pattern, name) for pattern in editor_patterns)
        
        # Check for translator patterns
        is_translator = any(re.search(pattern, name) for pattern in translator_patterns)
        
        # Determine role
        if is_editor:
            role = "editor"
        elif is_translator:
            role = "translator"
        else:
            role = "author"
        
        # Clean the name by removing role designations
        if is_editor:
            # Remove editor designations
            for pattern in editor_patterns:
                name = re.sub(pattern, '', name)
        
        if is_translator:
            # Remove translator designations
            for pattern in translator_patterns:
                name = re.sub(pattern, '', name)
        
        # Clean up remaining punctuation/whitespace
        name = re.sub(r'\(\s*\)', '', name)  # Empty parentheses
        name = re.sub(r'\s+', ' ', name)     # Multiple spaces
        name = re.sub(r'[\s,;:\.]+$', '', name)  # Trailing punctuation/whitespace
        name = re.sub(r'^[\s,;:\.]+', '', name)  # Leading punctuation/whitespace
        name = name.strip()
        
        if not name:
            return None, None, True
        
        # Check if this is a duplicate
        is_duplicate = name in seen_names_set
        if not is_duplicate:
            seen_names_set.add(name)
        
        return name, role, is_duplicate
    
    # Process P60327 field (contributor statement)
    contributor_statement = desc.find('./rdau:P60327', ns)
    if contributor_statement is not None and contributor_statement.text:
        statement_text = contributor_statement.text.strip()
        logger.debug(f"Author statement (P60327): {statement_text}")
        
        # First check for specific editorial patterns
        if "herausgegeben von" in statement_text:
            # Extract editors from the editorial statement
            editor_match = re.search(r'herausgegeben von\s+(.+?)(?:;|$)', statement_text)
            if editor_match:
                editors_text = editor_match.group(1).strip()
                # Split by "und" or "and" or commas
                editor_names = re.split(r'\s+(?:und|and)\s+|,\s*', editors_text)
                for name in editor_names:
                    clean_name, role, is_duplicate = process_name_with_role(name, seen_names)
                    if clean_name and not is_duplicate:
                        editors.append(clean_name)
                        logger.debug(f"Added editor from 'herausgegeben von': {clean_name}")
                
                # Remove the processed part to avoid duplication
                statement_text = statement_text.replace(editor_match.group(0), "")
                
        # Check for (Hg.) pattern - use a separate regex to catch all variations
        hg_pattern = re.compile(r'([^,;]+(?:\([Hh]g\.?\))[^,;]*)')
        editor_matches = hg_pattern.findall(statement_text)
        
        for match in editor_matches:
            clean_name, role, is_duplicate = process_name_with_role(match, seen_names)
            if clean_name and not is_duplicate and role == "editor":
                editors.append(clean_name)
                logger.debug(f"Added editor from (Hg.) pattern: {clean_name}")
                
            # Remove this match from the statement
            statement_text = statement_text.replace(match, '')
        
        # Clean up the statement after removing processed parts
        statement_text = re.sub(r'[,;]\s*[,;]', ',', statement_text)  # Clean up double separators
        statement_text = statement_text.strip(',; ')
        
        # Check for translator patterns
        if "Übers." in statement_text or "Übertragung" in statement_text or "übersetzt" in statement_text:
            # Extract translators
            trans_match = re.search(r'(?:Übers|Übertragung|übersetzt)[^:]*[:\.]\s*([^\.]+)', statement_text, re.IGNORECASE)
            if trans_match:
                translator_text = trans_match.group(1).strip()
                trans_names = re.split(r'\s+(?:und|and)\s+|,\s*', translator_text)
                for name in trans_names:
                    clean_name, role, is_duplicate = process_name_with_role(name, seen_names)
                    if clean_name and not is_duplicate:
                        translators.append(clean_name)
                        logger.debug(f"Added translator: {clean_name}")
                
                # Remove the processed part
                statement_text = statement_text.replace(trans_match.group(0), "")
        
        # Process any remaining names in the statement as authors
        if statement_text.strip():
            # Split by common separators
            name_parts = re.split(r',\s*|\s*;\s*|\s+und\s+|\s+and\s+', statement_text)
            for part in name_parts:
                if part.strip():
                    clean_name, role, is_duplicate = process_name_with_role(part, seen_names)
                    if clean_name and not is_duplicate:
                        if role == "editor":
                            editors.append(clean_name)
                            logger.debug(f"Added editor from remaining parts: {clean_name}")
                        elif role == "translator":
                            translators.append(clean_name)
                            logger.debug(f"Added translator from remaining parts: {clean_name}")
                        else:
                            authors.append(clean_name)
                            logger.debug(f"Added author from remaining parts: {clean_name}")
    
    # Extract authors from creator elements
    for creator_path in ['./dcterms:creator', './dc:creator']:
        creator_elems = desc.findall(creator_path, ns)
        for creator_elem in creator_elems:
            creator_resource = creator_elem.get('{'+ns['rdf']+'}resource')
            if creator_resource:
                logger.debug(f"Found creator resource: {creator_resource}")
                creator_desc = data.find(f'.//rdf:Description[ @rdf:about="{creator_resource}"]', ns)
                if creator_desc is not None:
                    name_elem = creator_desc.find('./gndo:preferredName', ns)
                    if name_elem is not None and name_elem.text:
                        clean_name, role, is_duplicate = process_name_with_role(name_elem.text, seen_names)
                        if clean_name and not is_duplicate:
                            if role == "editor":
                                editors.append(clean_name)
                                logger.debug(f"Added editor from resource: {clean_name}")
                            elif role == "translator":
                                translators.append(clean_name)
                                logger.debug(f"Added translator from resource: {clean_name}")
                            else:
                                authors.append(clean_name)
                                logger.debug(f"Added author from resource: {clean_name}")
                continue
                
            # If creator contains text directly
            if creator_elem.text and creator_elem.text.strip():
                clean_name, role, is_duplicate = process_name_with_role(creator_elem.text, seen_names)
                if clean_name and not is_duplicate:
                    if role == "editor":
                        editors.append(clean_name)
                        logger.debug(f"Added editor from direct text: {clean_name}")
                    elif role == "translator":
                        translators.append(clean_name)
                        logger.debug(f"Added translator from direct text: {clean_name}")
                    else:
                        authors.append(clean_name)
                        logger.debug(f"Added author from direct text: {clean_name}")
                continue
                
            # If creator contains nested elements
            nested_nodes = creator_elem.findall('.//*', ns)
            for node in nested_nodes:
                if 'preferredName' in node.tag and node.text:
                    clean_name, role, is_duplicate = process_name_with_role(node.text, seen_names)
                    if clean_name and not is_duplicate:
                        if role == "editor":
                            editors.append(clean_name)
                            logger.debug(f"Added editor from nested element: {clean_name}")
                        elif role == "translator":
                            translators.append(clean_name)
                            logger.debug(f"Added translator from nested element: {clean_name}")
                        else:
                            authors.append(clean_name)
                            logger.debug(f"Added author from nested element: {clean_name}")
                    break
    
    # Extract authors from marcRole elements - using specific role codes
    role_mapping = {
        'aut': 'author',
        'cre': 'author',
        'edt': 'editor',
        'hrg': 'editor',  # German editor role
        'trl': 'translator',
        'ths': 'author',  # Thesis advisor typically listed as author
        'ctb': 'contributor'  # General contributor
    }
    
    for role_code, role_type in role_mapping.items():
        role_elems = desc.findall(f'./marcRole:{role_code}', ns)
        for role_elem in role_elems:
            # Resource reference
            resource = role_elem.get('{'+ns['rdf']+'}resource')
            if resource:
                desc_elem = data.find(f'.//rdf:Description[ @rdf:about="{resource}"]', ns)
                if desc_elem is not None:
                    name_elem = desc_elem.find('./gndo:preferredName', ns)
                    if name_elem is not None and name_elem.text:
                        clean_name, detected_role, is_duplicate = process_name_with_role(name_elem.text, seen_names)
                        # Use the role from marcRole if detected_role is "author" (default)
                        # Otherwise, use the detected role (e.g., if name contains editor pattern)
                        actual_role = detected_role if detected_role != "author" else role_type
                        
                        if clean_name and not is_duplicate:
                            if actual_role == "editor":
                                editors.append(clean_name)
                                logger.debug(f"Added editor from marcRole:{role_code}: {clean_name}")
                            elif actual_role == "translator":
                                translators.append(clean_name)
                                logger.debug(f"Added translator from marcRole:{role_code}: {clean_name}")
                            elif actual_role == "author":
                                authors.append(clean_name)
                                logger.debug(f"Added author from marcRole:{role_code}: {clean_name}")
                            else:
                                contributors.append({"name": clean_name, "role": actual_role})
                                logger.debug(f"Added contributor from marcRole:{role_code}: {clean_name}")
                continue
            
            # Nested description
            for node_desc in role_elem.findall('./rdf:Description', ns):
                name_elem = node_desc.find('./gndo:preferredName', ns)
                if name_elem is not None and name_elem.text:
                    clean_name, detected_role, is_duplicate = process_name_with_role(name_elem.text, seen_names)
                    actual_role = detected_role if detected_role != "author" else role_type
                    
                    if clean_name and not is_duplicate:
                        if actual_role == "editor":
                            editors.append(clean_name)
                            logger.debug(f"Added editor from nested marcRole:{role_code}: {clean_name}")
                        elif actual_role == "translator":
                            translators.append(clean_name)
                            logger.debug(f"Added translator from nested marcRole:{role_code}: {clean_name}")
                        elif actual_role == "author":
                            authors.append(clean_name)
                            logger.debug(f"Added author from nested marcRole:{role_code}: {clean_name}")
                        else:
                            contributors.append({"name": clean_name, "role": actual_role})
                            logger.debug(f"Added contributor from nested marcRole:{role_code}: {clean_name}")
            
            # NodeID reference
            node_id = role_elem.get('{'+ns['rdf']+'}nodeID')
            if node_id:
                node_desc = data.find(f'.//rdf:Description[ @rdf:nodeID="{node_id}"]', ns)
                if node_desc is not None:
                    name_elem = node_desc.find('./gndo:preferredName', ns)
                    if name_elem is not None and name_elem.text:
                        clean_name, detected_role, is_duplicate = process_name_with_role(name_elem.text, seen_names)
                        actual_role = detected_role if detected_role != "author" else role_type
                        
                        if clean_name and not is_duplicate:
                            if actual_role == "editor":
                                editors.append(clean_name)
                                logger.debug(f"Added editor from marcRole:{role_code} nodeID: {clean_name}")
                            elif actual_role == "translator":
                                translators.append(clean_name)
                                logger.debug(f"Added translator from marcRole:{role_code} nodeID: {clean_name}")
                            elif actual_role == "author":
                                authors.append(clean_name)
                                logger.debug(f"Added author from marcRole:{role_code} nodeID: {clean_name}")
                            else:
                                contributors.append({"name": clean_name, "role": actual_role})
                                logger.debug(f"Added contributor from marcRole:{role_code} nodeID: {clean_name}")
    
    logger.debug(f"Final author list: {authors}")
    logger.debug(f"Final editor list: {editors}")
    logger.debug(f"Final translator list: {translators}")
    
    # Find year
    year = None
    issued_elem = desc.find('./dcterms:issued', ns)
    if issued_elem is not None and issued_elem.text:
        # Extract year
        match = re.search(r'\b(1\d{3}|20\d{2})\b', issued_elem.text)
        if match:
            year = match.group(1)
            logger.debug(f"Found year: {year}")
    
    # Find publisher - separately handling name and place
    publisher_name = None
    publisher_elem = desc.find('./dc:publisher', ns)
    if publisher_elem is not None and publisher_elem.text:
        publisher_name = publisher_elem.text.strip()
        logger.debug(f"Found publisher: {publisher_name}")
    
    # Find place of publication
    places = []
    place_elems = desc.findall('./rdau:P60163', ns)
    for place_elem in place_elems:
        if place_elem is not None and place_elem.text and place_elem.text.strip():
            places.append(place_elem.text.strip())
    
    place_of_publication = ", ".join(places) if places else None
    if place_of_publication:
        logger.debug(f"Found place of publication: {place_of_publication}")
    
    # Check for publication statement that might have both
    pub_statement = desc.find('./rdau:P60333', ns)
    if pub_statement is not None and pub_statement.text:
        statement = pub_statement.text.strip()
        logger.debug(f"Found publication statement: {statement}")
        if not place_of_publication or not publisher_name:
            parts = statement.split(" : ", 1)
            if len(parts) > 1:
                if not place_of_publication:
                    place_of_publication = parts[0].strip()
                    logger.debug(f"Extracted place from statement: {place_of_publication}")
                if not publisher_name:
                    pub_part = parts[1].strip()
                    pub_part = re.sub(r',?\s*\[\d{4}\]$', '', pub_part)
                    publisher_name = pub_part
                    logger.debug(f"Extracted publisher from statement: {publisher_name}")
    
    # Find edition
    edition = None
    edition_elem = desc.find('./bibo:edition', ns)
    if edition_elem is not None and edition_elem.text:
        edition = edition_elem.text.strip()
        logger.debug(f"Found edition: {edition}")
    
    # Find extent (number of pages, etc.)
    extent = None
    extent_elem = desc.find('./isbd:P1053', ns) or desc.find('./dcterms:extent', ns)
    if extent_elem is not None and extent_elem.text:
        extent = extent_elem.text.strip()
        logger.debug(f"Found extent: {extent}")
    
    # Try to extract page info from extent
    pages = None
    if extent:
        page_match = re.search(r'(\d+)(?:\s*[-–]\s*(\d+))?\s*(?:p|pages|S)', extent, re.IGNORECASE)
        if page_match:
            if page_match.group(2):  # Range
                pages = f"{page_match.group(1)}-{page_match.group(2)}"
            else:  # Single page count
                pages = page_match.group(1)
            logger.debug(f"Extracted pages from extent: {pages}")
    
    # Find document type
    document_type = None
    type_elem = desc.find('./dcterms:type', ns) or desc.find('./dc:type', ns)
    if type_elem is not None:
        # Check for resource reference
        resource = type_elem.get('{'+ns['rdf']+'}resource')
        if resource:
            # Extract type from URI
            type_parts = resource.split('/')
            if type_parts:
                document_type = type_parts[-1]
                logger.debug(f"Found document type from resource: {document_type}")
        # Or direct text
        elif type_elem.text:
            document_type = type_elem.text.strip()
            logger.debug(f"Found document type from text: {document_type}")
    
    # Find series and journal info
    series = None
    journal_title = None
    volume = None
    issue = None
    
    # Check for series
    series_elem = desc.find('./dcterms:isPartOf', ns)
    if series_elem is not None:
        # Text content
        if series_elem.text:
            series = series_elem.text.strip()
            logger.debug(f"Found series: {series}")
        # Resource reference
        else:
            resource = series_elem.get('{'+ns['rdf']+'}resource')
            if resource:
                logger.debug(f"Found series resource: {resource}")
                # Find the referenced resource
                series_desc = data.find(f'.//rdf:Description[ @rdf:about="{resource}"]', ns)
                if series_desc is not None:
                    # Try to get the title
                    title_elem = series_desc.find('./dc:title', ns) or series_desc.find('./dcterms:title', ns)
                    if title_elem is not None and title_elem.text:
                        series = title_elem.text.strip()
                        logger.debug(f"Extracted series from resource: {series}")
    
    # Check for biblio:Journal relation
    journal_elem = desc.find('./bibo:Journal', ns)
    if journal_elem is not None:
        resource = journal_elem.get('{'+ns['rdf']+'}resource')
        if resource:
            logger.debug(f"Found journal resource: {resource}")
            # Find the referenced resource
            journal_desc = data.find(f'.//rdf:Description[ @rdf:about="{resource}"]', ns)
            if journal_desc is not None:
                # Try to get the title
                title_elem = journal_desc.find('./dc:title', ns) or journal_desc.find('./dcterms:title', ns)
                if title_elem is not None and title_elem.text:
                    journal_title = title_elem.text.strip()
                    document_type = "Journal Article"
                    logger.debug(f"Found journal title: {journal_title}")
    
    # Volume and issue
    volume_elem = desc.find('./bibo:volume', ns)
    if volume_elem is not None and volume_elem.text:
        volume = volume_elem.text.strip()
        logger.debug(f"Found volume: {volume}")
    
    issue_elem = desc.find('./bibo:issue', ns)
    if issue_elem is not None and issue_elem.text:
        issue = issue_elem.text.strip()
        logger.debug(f"Found issue: {issue}")
    
    # Check if this is a book chapter
    chapter_elem = desc.find('./bibo:chapter', ns)
    if chapter_elem is not None or (document_type and "chapter" in document_type.lower()):
        document_type = "Book Chapter"
        logger.debug("Determined document is a book chapter")
        
        # Series field will be used for book title
    
    # Find ISBN
    isbn = None
    for isbn_field in ['isbn13', 'isbn10', 'isbn', 'gtin14']:
        isbn_elem = desc.find(f'./bibo:{isbn_field}', ns)
        if isbn_elem is not None and isbn_elem.text:
            isbn = isbn_elem.text.strip()
            logger.debug(f"Found ISBN ({isbn_field}): {isbn}")
            break
    
    # Find ISSN
    issn = None
    issn_elem = desc.find('./bibo:issn', ns)
    if issn_elem is not None and issn_elem.text:
        issn = issn_elem.text.strip()
        logger.debug(f"Found ISSN: {issn}")
    
    # Find DOI
    doi = None
    doi_elem = desc.find('./bibo:doi', ns)
    if doi_elem is not None and doi_elem.text:
        doi = doi_elem.text.strip()
        logger.debug(f"Found DOI: {doi}")
    
    # Find subjects
    subjects = []
    seen_subjects = set()
    subject_elems = desc.findall('./dcterms:subject', ns)
    for elem in subject_elems:
        resource = elem.get('{'+ns['rdf']+'}resource')
        if resource:
            subject = resource.split('/')[-1]
            if subject and subject not in seen_subjects:
                subjects.append(subject)
                seen_subjects.add(subject)
                logger.debug(f"Found subject from resource: {subject}")
                continue
        
        if elem.text and elem.text.strip():
            subject = elem.text.strip()
            if subject not in seen_subjects:
                subjects.append(subject)
                seen_subjects.add(subject)
                logger.debug(f"Found subject from text: {subject}")
    
    # Also check dc:subject
    dc_subject_elems = desc.findall('./dc:subject', ns)
    for elem in dc_subject_elems:
        if elem.text and elem.text.strip():
            subject = elem.text.strip()
            if subject not in seen_subjects:
                subjects.append(subject)
                seen_subjects.add(subject)
                logger.debug(f"Found dc:subject: {subject}")
    
    logger.debug(f"Found {len(subjects)} subjects")
    
    # Find language
    language = None
    language_elem = desc.find('./dcterms:language', ns)
    if language_elem is not None:
        resource = language_elem.get('{'+ns['rdf']+'}resource')
        if resource:
            parts = resource.split('/')
            if parts:
                language = parts[-1]
                logger.debug(f"Found language from resource: {language}")
        elif language_elem.text and language_elem.text.strip():
            language = language_elem.text.strip()
            logger.debug(f"Found language from text: {language}")
    
    # Find abstract/description
    abstract = None
    for desc_tag in ['description', 'abstract', 'P60493']:
        for ns_prefix in ['dc', 'dcterms', 'rdau']:
            desc_elem = desc.find(f'./{ns_prefix}:{desc_tag}', ns)
            if desc_elem is not None and desc_elem.text:
                abstract = desc_elem.text.strip()
                logger.debug(f"Found abstract from {ns_prefix}:{desc_tag}: {abstract[:100]}...")
                break
        if abstract:
            break
    
    # Find URLs
    urls = []
    seen_urls = set()
    
    for primaryTopic_elem in desc.findall('./foaf:primaryTopic', ns):
        resource = primaryTopic_elem.get('{'+ns['rdf']+'}resource')
        if resource and resource.startswith('http') and resource not in seen_urls:
            urls.append(resource)
            seen_urls.add(resource)
            logger.debug(f"Found URL from primaryTopic: {resource}")
    
    for like_elem in desc.findall('./umbel:isLike', ns):
        resource = like_elem.get('{'+ns['rdf']+'}resource')
        if resource and resource.startswith('http') and resource not in seen_urls:
            urls.append(resource)
            seen_urls.add(resource)
            logger.debug(f"Found URL from isLike: {resource}")
    
    logger.debug(f"Found {len(urls)} URLs")
    
    # Log record summary
    logger.debug(f"Record summary for {record_id}:")
    logger.debug(f"  Title: {title}")
    logger.debug(f"  Author count: {len(authors)}")
    logger.debug(f"  Editor count: {len(editors)}")
    logger.debug(f"  Year: {year}")
    logger.debug(f"  Publisher: {publisher_name}")
    logger.debug(f"  Type: {document_type}")
    
    # Create and return BiblioRecord
    return BiblioRecord(
        id=record_id,
        title=title,
        authors=authors,
        editors=editors,
        translators=translators,
        contributors=contributors,
        year=year,
        publisher_name=publisher_name,
        place_of_publication=place_of_publication,
        isbn=isbn,
        issn=issn,
        urls=urls,
        abstract=abstract,
        language=language,
        format=document_type,
        subjects=subjects,
        series=series,
        extent=extent,
        edition=edition,
        journal_title=journal_title,
        volume=volume,
        issue=issue,
        pages=pages,
        doi=doi,
        document_type=document_type,
        raw_data=raw_record['raw_xml'],
        schema=raw_record.get('schema')
    )

# Add function to generate BibTeX from BiblioRecord
def bibtex_from_record(record: BiblioRecord) -> str:
    """
    Convert a BiblioRecord to BibTeX format.
    
    Args:
        record: BiblioRecord object
        
    Returns:
        BibTeX formatted string
    """
    # Get citation key from record
    citation_key = record.get_citation_key()
    
    # Determine entry type
    if record.document_type:
        doc_type_lower = record.document_type.lower()
        if "article" in doc_type_lower:
            entry_type = "article"
        elif "chapter" in doc_type_lower:
            entry_type = "incollection"
        elif "thesis" in doc_type_lower:
            entry_type = "phdthesis"
        elif "proceedings" in doc_type_lower:
            entry_type = "inproceedings"
        elif "report" in doc_type_lower:
            entry_type = "techreport"
        else:
            entry_type = "book"
    else:
        # Default to book if no document type specified
        entry_type = "book"
    
    # Start building BibTeX
    bibtex = [f"@{entry_type}{{{citation_key},"]
    
    # Clean up the title
    # Remove trailing author information after '/'
    title = re.sub(r'\s*/\s*[^/]+$', '', record.title)
    # Escape special characters
    title = title.replace("{", "\{").replace("}", "\}")
    bibtex.append(f"  title = {{{title}}},")
    
    # Clean and add authors if available
    if record.authors:
        # Clean up author names
        cleaned_authors = []
        for author in record.authors:
            # Remove role indicators
            clean_author = re.sub(r'\s*\[[^\]]*\]', '', author)
            # Remove trailing commas and whitespace
            clean_author = re.sub(r',\s*$', '', clean_author.strip())
            # Fix any broken bracket pairs
            clean_author = re.sub(r'\]\s*$', '', clean_author)
            clean_author = re.sub(r'^\s*\[', '', clean_author)
            
            if clean_author:
                cleaned_authors.append(clean_author)
        
        if cleaned_authors:
            # Format authors properly for BibTeX
            authors_list = " and ".join(cleaned_authors)
            bibtex.append(f"  author = {{{authors_list}}},")
    
    # Add editors if available
    if record.editors:
        # Clean up editor names
        cleaned_editors = []
        for editor in record.editors:
            # Remove role indicators
            clean_editor = re.sub(r'\s*\[[^\]]*\]', '', editor)
            # Remove trailing commas and whitespace
            clean_editor = re.sub(r',\s*$', '', clean_editor.strip())
            # Fix any broken bracket pairs
            clean_editor = re.sub(r'\]\s*$', '', clean_editor)
            clean_editor = re.sub(r'^\s*\[', '', clean_editor)
            
            if clean_editor:
                cleaned_editors.append(clean_editor)
        
        if cleaned_editors:
            # Format editors properly for BibTeX
            editors_list = " and ".join(cleaned_editors)
            bibtex.append(f"  editor = {{{editors_list}}},")
    
    # Add translators if available
    if record.translators:
        # Clean up translator names
        cleaned_translators = []
        for translator in record.translators:
            # Remove role indicators
            clean_translator = re.sub(r'\s*\[[^\]]*\]', '', translator)
            # Remove trailing commas and whitespace
            clean_translator = re.sub(r',\s*$', '', clean_translator.strip())
            # Fix any broken bracket pairs
            clean_translator = re.sub(r'\]\s*$', '', clean_translator)
            clean_translator = re.sub(r'^\s*\[', '', clean_translator)
            
            if clean_translator:
                cleaned_translators.append(clean_translator)
        
        if cleaned_translators:
            # Add translators in note field (BibTeX doesn't have a translator field)
            translators_list = " and ".join(cleaned_translators)
            bibtex.append(f"  translator = {{{translators_list}}},")
    
    # Year
    if record.year:
        bibtex.append(f"  year = {{{record.year}}},")
    
    # Journal for articles
    if entry_type == "article" and record.journal_title:
        bibtex.append(f"  journal = {{{record.journal_title}}},")
        
        # Volume
        if record.volume:
            bibtex.append(f"  volume = {{{record.volume}}},")
            
        # Issue/Number
        if record.issue:
            bibtex.append(f"  number = {{{record.issue}}},")
    
    # Publisher
    if record.publisher_name:
        bibtex.append(f"  publisher = {{{record.publisher_name}}},")
    
    # Address (place of publication)
    if record.place_of_publication:
        bibtex.append(f"  address = {{{record.place_of_publication}}},")
    
    # Series
    if record.series:
        bibtex.append(f"  series = {{{record.series}}},")
    
    # ISBN
    if record.isbn:
        bibtex.append(f"  isbn = {{{record.isbn}}},")
    
    # ISSN for journals
    if entry_type == "article" and record.issn:
        bibtex.append(f"  issn = {{{record.issn}}},")
    
    # DOI
    if record.doi:
        bibtex.append(f"  doi = {{{record.doi}}},")
    
    # Pages
    if record.pages:
        bibtex.append(f"  pages = {{{record.pages}}},")
    
    # Edition
    if record.edition:
        bibtex.append(f"  edition = {{{record.edition}}},")
    
    # URL (use the first one if multiple are available)
    if record.urls:
        bibtex.append(f"  url = {{{record.urls[0]}}},")
    
    # Language
    if record.language:
        bibtex.append(f"  language = {{{record.language}}},")
    
    # Put record ID in note field for reference
    bibtex.append(f"  note = {{ID: {record.id}}}")
    
    # Close the entry
    bibtex.append("}")
    
    return "\n".join(bibtex)

# Function to convert a list of BiblioRecords to BibTeX format
def bibtex_from_records(records: List[BiblioRecord]) -> str:
    """
    Convert a list of BiblioRecords to BibTeX format with proper handling
    for duplicate keys.
    
    Args:
        records: List of BiblioRecord objects
        
    Returns:
        BibTeX formatted string with all records
    """
    results = []
    used_keys = set()
    
    for i, record in enumerate(records):
        # Get base citation key and ensure uniqueness
        base_key = record.get_citation_key()
        # Clean up citation key to avoid problematic characters
        base_key = re.sub(r'[^a-zA-Z0-9]', '', base_key)
        
        # If key is empty (e.g., no author), use "unknown"
        if not base_key:
            base_key = "unknown"
            
        citation_key = base_key
        
        # If key already exists, add a suffix
        if citation_key in used_keys:
            j = 1
            while f"{citation_key}{j}" in used_keys:
                j += 1
            citation_key = f"{citation_key}{j}"
        
        used_keys.add(citation_key)
        
        # Create a copy of the record with the new key
        record_copy = BiblioRecord(
            id=citation_key,
            title=record.title,
            authors=record.authors.copy() if record.authors else [],
            editors=record.editors.copy() if record.editors else [],
            translators=record.translators.copy() if record.translators else [],
            contributors=record.contributors.copy() if record.contributors else [],
            year=record.year,
            publisher_name=record.publisher_name,
            place_of_publication=record.place_of_publication,
            isbn=record.isbn,
            issn=record.issn,
            urls=record.urls.copy() if record.urls else [],
            abstract=record.abstract,
            language=record.language,
            format=record.format,
            subjects=record.subjects.copy() if record.subjects else [],
            series=record.series,
            extent=record.extent,
            edition=record.edition,
            journal_title=record.journal_title,
            volume=record.volume,
            issue=record.issue,
            pages=record.pages,
            doi=record.doi,
            document_type=record.document_type,
            raw_data=record.raw_data,
            schema=record.schema
        )
        
        # Add BibTeX for this record
        results.append(bibtex_from_record(record_copy))
        
        # Add a separator between records
        if i < len(records) - 1:
            results.append("")
    
    return "\n".join(results)

# List of commonly used SRU endpoints
SRU_ENDPOINTS = {
    # National Libraries
    'dnb': {
        'name': 'Deutsche Nationalbibliothek',
        'url': 'https://services.dnb.de/sru/dnb',
        'default_schema': 'RDFxml',
        'description': 'The German National Library',
        'version': '1.1',
        'examples': {
            'title': 'TIT=Python',
            'author': 'PER=Einstein',
            'isbn': 'ISBN=9783658310844',
            'advanced': {'TIT': 'Python', 'JHR': '2023'}
        }
    },
    'bnf': {
        'name': 'Bibliothèque nationale de France',
        'url': 'http://catalogue.bnf.fr/api/SRU',
        'default_schema': 'dublincore',  # Important: changed from marcxchange
        'description': 'The French National Library',
        'version': '1.2',
        'examples': {
            'title': 'bib.title any "Python"',  # Changed from 'all' to 'any'
            'author': 'bib.author any "Einstein"',  # Changed from 'all' to 'any'
            'isbn': 'bib.isbn any "9782012919198"',
            'advanced': 'bib.title any "Python" and bib.date any "2023"'
        }
    },
    'zdb': {
        'name': 'ZDB - German Union Catalogue of Serials',
        'url': 'https://services.dnb.de/sru/zdb',
        'default_schema': 'MARC21-xml',
        'description': 'German Union Catalogue of Serials',
        'version': '1.1',
        'examples': {
            'title': 'TIT=Journal',
            'issn': 'ISS=0740-171x',
            'advanced': {'TIT': 'Journal', 'JHR': '2023'}
        }
    },
    'loc': {
        'name': 'Library of Congress',
        'url': 'https://lccn.loc.gov/sru',
        'default_schema': 'marcxml',
        'description': 'Library of Congress catalog',
        'version': '1.1',
        'examples': {
            'title': 'title="Python"',
            'author': 'author="Einstein"',
            'isbn': 'isbn=9781234567890',
            'advanced': 'title="Python" and author="Rossum"'
        }
    },
    
    # Other libraries and collections
    'trove': {
        'name': 'Trove (National Library of Australia)',
        'url': 'http://www.nla.gov.au/apps/srw/search/peopleaustralia',
        'default_schema': 'dc',
        'description': "Australia's cultural collections",
        'version': '1.1',
        'examples': {
            'name': 'bath.name="Smith"',
            'advanced': 'pa.surname="Smith" and pa.firstname="John"'
        }
    },
    'kb': {
        'name': 'KB - National Library of the Netherlands',
        'url': 'http://jsru.kb.nl/sru',
        'default_schema': 'dc',
        'description': 'Dutch National Library',
        'version': '1.1',
        'examples': {
            'title': 'dc.title=Python',
            'advanced': 'dc.title=Python and dc.date=2023'
        }
    },
    'bibsys': {
        'name': 'BIBSYS - Norwegian Library Service',
        'url': 'http://sru.bibsys.no/search/biblio',
        'default_schema': 'dc',
        'description': 'Norwegian academic libraries',
        'version': '1.1',
        'examples': {
            'title': 'title="Python"',
            'author': 'author="Einstein"',
            'advanced': 'title="Python" and date="2023"'
        }
    }
}