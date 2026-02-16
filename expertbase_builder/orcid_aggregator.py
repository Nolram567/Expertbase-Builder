import csv
import logging
from datetime import date

import requests

BASE_URL = "https://pub.orcid.org/v3.0/"

logger = logging.getLogger(__name__)

def read_orcids_from_csv(file_path: str) -> list[str]:
    """
    Liest ORCID-Bezeichner aus der zweiten Spalte einer CSV-Datei ein.

    Args:
        file_path: Pfad zur CSV-Datei.

    Returns:
        Liste der ORCID-Bezeichner.
    """
    orcids = []
    try:
        with open(file_path, newline='', encoding='utf-8') as csvfile:
            reader = csv.reader(csvfile)
            next(reader)
            for row in reader:
                orcids.append(row[1].strip())
    except IOError as e:
        logger.error(f"Die Datei {file_path} konnte nicht geöffnet werden:\n {e}")
        raise

    return orcids

def fetch_orcid_data(orcid: str, endpoint: str) -> dict | None:
    """
    Fragt Daten für eine Person über die ORCID API ab.

    Args:
        orcid: ORCID-Bezeichner.
        endpoint: Der Endpunkt, der abgefragt werden soll.
    Returns:
        ORCID-Daten als Dictionary oder None bei Fehler.
    """
    headers = {"Accept": "application/json"}
    url = f"{BASE_URL}{orcid}/{endpoint}"
    response = requests.get(url, headers=headers)

    if response.status_code == 200:
        return response.json()
    else:
        logger.warning(f"Fehler beim Abrufen von {orcid}. HTTP-Statuscode: {response.status_code}")
        return None

def extract_names(orcid_data: dict | None) -> dict[str, str]:
    """
    Extrahiert die Namen aus einem Personendatensatz.

    Args:
        orcid_data: Die ORCID-Daten vom /person-Endpunkt der ORCID-API.

    Returns:
        Extrahierte Namen als Dictionary.
    """
    extracted = {}

    names = orcid_data.get("name", {})
    extracted["given-names"] = names.get("given-names", {}).get("value", "")
    extracted["family-name"] = names.get("family-name", {}).get("value", "")

    return extracted

def extract_mail(orcid_data: dict | None) -> str:
    """
    Extrahiert die erste E-Mail-Adresse aus einem Personendatensatz.

    Args:
        orcid_data: Die ORCID-Daten vom /person-Endpunkt der ORCID-API.
    Returns:
        Die E-Mail als String.
    """
    emails = orcid_data.get("emails", {}).get("email", [])
    email_str = emails[0].get("email", "") if emails else ""
    return email_str

def extract_current_employments(orcid_data: dict | None) -> list[str]:
    """
    Extrahiert die derzeit aktiven Beschäftigungsverhältnisse aus einem Personendatensatz.

    Args:
        orcid_data: Die ORCID-Daten vom /activities-Endpunkt der ORCID-API.

    Returns:
        Extrahierte Beschäftigungsverhältnisse als Dictionary.
    """

    current_employments = []

    for employment in orcid_data.get("employments", {}).get("affiliation-group", []):

        current_summary = employment.get("summaries", [])[0].get("employment-summary", {})

        # Wenn das Enddatum nicht definiert ist
        if current_summary["end-date"] is None:
            current_employments.append([current_summary.get("role-title", ""),
                                        current_summary.get("department-name", ""),
                                        current_summary.get("organization", {}).get("name", "")]
                                       )
            continue

        # überspringen, wenn das Enddatum in der Vergangenheit liegt
        if int(current_summary["end-date"]["year"]['value']) < date.today().year:
            continue

        # Wenn das Enddatum in der Zukunft liegt
        if current_summary is None:
            continue

        end = current_summary.get('end-date') or {}
        year_val = (end.get('year') or {}).get('value')
        month_val = (end.get('month') or {}).get('value', 1)
        day_val = (end.get('day') or {}).get('value', 1)

        year = int(year_val) if year_val is not None else 2050
        month = int(month_val) if month_val is not None else 1
        day = int(day_val) if day_val is not None else 1

        given_date = date(year, month, day)
        today = date.today()

        if given_date > today:
            current_employments.append([current_summary.get("role-title", ""),
                                        current_summary.get("department-name", ""),
                                        current_summary.get("organization", {}).get("name", "")]
                                       )

    return current_employments

def extract_keywords(orcid_data: dict) -> list[str]:
    """
    Extrahiert eine Liste von Keywords aus einem Personendatensatz.

    Args:
       orcid_data: Die ORCID-Daten vom /person-Endpunkt der ORCID-API.

    Returns: Eine Liste mit den Keywords der jeweiligen Person als Strings.
    """
    keywords = []

    for k in orcid_data["keywords"]["keyword"]:
        keywords.append(k["content"])
    return keywords
