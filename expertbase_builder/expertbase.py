import os
import json
import logging
from pathlib import Path

from .orcid_aggregator import *
from .expert import Expert
import yaml

logger = logging.getLogger(__name__)

def create_tadirah_map(file_path: str) -> dict:
    """
    Erstellt ein Dictionary, das die Orcids in der übergebenen Datei auf die korrespondierenden tadirah-Schlagwörter
    abbildet.

    Args:
        file_path: Der Dateipfad zu der CSV-Datei.

    Returns:
        Ein Dictionary, das die ORCIDS auf die tadirah-Schlagwörter abbildet.
    """
    orcids = []
    tadirah = []

    with open(Path(file_path), newline='', encoding='utf-8') as csvfile:
        reader = csv.reader(csvfile)
        next(reader)
        for row in reader:
            orcids.append(row[1].strip())
            if "," in row[2]:
                tadirah_list = row[2].split(",")
                tadirah.append([t.strip() for t in tadirah_list])

    return dict(zip(orcids, tadirah))

class ExpertBase:
    """
    Objekte dieser Klasse repräsentieren die Expertbase als Collection von Expert-Objekten.

    Diese Klasse stellt Methoden zur Verfügung, um die Expertbase zu erstellen, zu verwalten und zu serialisieren.
    Die Expertbase wird in der Objektvariable "base" als Dictionary nach folgendem Muster verwaltet:
    {orcid: Objekt der Klasse Experte,
    (...)
    }
    """

    def __init__(self, filename: str, from_csv: bool = True):
        """
        Der Konstruktor der Klasse enthält eine Fallunterscheidung. Entweder wird das ExpertBase-Objekt auf der Grundlage
        von einer CSV-Datei gefüllt oder aus dem Speicher geladen.

        Args:
            filename: Der Name der Quelldatei.
            from_csv: Wenn True, dann wird das Objekt mit einer CSV-Datei befüllt.
        """
        self.raw_base = {}
        self.base = {}

        if from_csv:
            self.populate_from_csv(filename)
        else:
            self.deserialize_expertbase(filename)

    def populate_from_csv(self, path: str) -> None:
        """
        Diese Methode füllt das ExpertBase-Objekt auf Grundlage einer CSV-Datei mit ORCID's.
        Die CSV-Datei muss die folgende Struktur haben:\n
        |Spaltenname1|Spaltenname2|\n
        |    Name    |   Orcid    |

        Args:
            path: Der Dateipfad zu der CSV-Datei.
        """

        orcids = read_orcids_from_csv(path)
        tadirah_map = create_tadirah_map(path)

        logger.info(f"Das ExpertBase-Objekt wird mit den ORCID's aus {path} befüllt.")

        for orcid in orcids:
            logger.info(f"Abfrage von ORCID {orcid}...")
            person_endpoint_data = fetch_orcid_data(orcid, endpoint="person")
            activities_endpoint_data = fetch_orcid_data(orcid, endpoint="activities")


            if person_endpoint_data is None or activities_endpoint_data is None:
                logger.error(f"Fehler beim Abrufen von Daten oder leere Antwort für ORCID {orcid}")
                continue

            extracted_name = extract_names(person_endpoint_data)
            extracted_keywords = extract_keywords(person_endpoint_data)
            extracted_employment = extract_current_employments(activities_endpoint_data)
            extracted_mail = extract_mail(person_endpoint_data)

            new_expert = Expert(orcid=orcid,
                                data={
                                    "Vorname": extracted_name["given-names"],
                                    "Nachname": extracted_name["family-name"],
                                    "Derzeitige Beschäftigung": extracted_employment,
                                    "Forschungsinteressen": extracted_keywords,
                                    "E-Mail": extracted_mail,
                                    "TaDiRAH-Zuordnung": tadirah_map[orcid]
                                })

            self.base[orcid] = new_expert
            self.raw_base[orcid] = new_expert.get_properties()

        logger.info(f"Das Expertbase-Objekt wurde erfolgreich mit den ORCID's aus {path} befüllt.")

    def get_base(self) -> dict:
        """
        Gibt eine einfache Kopie der Objektvariable base zurück.
        """
        return self.base.copy()

    def get_expert_as_list(self) -> list[Expert]:
        """
        Die Methode gibt alle Experten-Objekte des Expertbase-Objekts als Liste zurück.
        """
        return list(self.base.values())

    def get_orcids_as_list(self) -> list[str]:
        """
        Die Methode gibt die ORCIDs aller Experten in der Expertbase zurück.
        """
        return list(self.base.keys())

    def deserialize_expertbase(self, path: str) -> None:
        """
        Die Methode deserialisiert ein Expertbase-Objekt.

        Args:
            path: Der Dateipfad, unter dem das Expertbase-Objekt gespeichert ist.
        """
        try:
            with open(path, "r", encoding='utf-8') as f:
                self.raw_base = json.load(f)

            for orcid, expert in self.raw_base.items():

                new_expert = Expert(orcid=orcid,
                                    data={
                                        "Vorname": expert.get("Vorname", ""),
                                        "Nachname": expert.get("Nachname", ""),
                                        "Derzeitige Beschäftigung": expert.get("Derzeitige Beschäftigung", []),
                                        "Forschungsinteressen": expert.get("Forschungsinteressen", []),
                                        "E-Mail": expert.get("E-Mail", "")
                                    })

                self.base[orcid] = new_expert

            logger.info(f"Das Expertbase-Objekt wurde erfolgreich von {path} eingelesen.")

        except IOError as e:
            logger.error(f"Fehler beim Deserialisieren der Expertbase unter {path}:\n{e}")
            raise

    def serialize_expertbase(self, path: str, name: str) -> None:
        """
        Diese Methode serialisiert das Expertbase-Objekt als JSON-Datei.

        Args:
            path: Der Dateipfad, unter dem das Expertbase Objekt serialisiert werden soll.
            name: Der Name der Datei.
        """

        os.makedirs(path, exist_ok=True)

        with open(os.path.join(path, name), "w", encoding='utf-8') as f:
            json.dump(self.raw_base, f, indent=4, ensure_ascii=False)

        logger.info(f"Das Expertbase-Objekt wurde erfolgreich unter {path} serialisiert.")

    def pretty_print(self) -> None:
        """
        Diese Methode druckt die Expertbase menschenlesbar auf der Konsole.
        """
        print(json.dumps(self.raw_base, indent=4, ensure_ascii=False))

    def parse_yml(self, path: str, filename: str = "expertbase.yml") -> None:
        """
        Diese Methode parst ein Expertbase-Objekt zu einer yaml-Datei, die mit quarto listings kompatibel ist.

        Args:
            path: Der Dateipfad und der Name der Ausgabedatei.
            name: Der Name des Objekts.
            filename: Der Dateiname der Ausgabedatei.
        """

        logger.info(f"Das Expertbase-Objekt wird zu einer YAML-Datei geparst.")

        entries = []

        for expert in self.get_expert_as_list():

            name = expert.get_name(formated=False)
            research_interest = expert.get_research_interest(formated=True)
            personal_page = f"experts/{name[0].lower().strip().replace(" ", "-")}-{name[1].lower().strip().replace(" ", "-")}.html"
            linked_name = f'<a href={personal_page}>{expert.get_name(formated=True)}</a>'
            organisation = ",<br>".join(expert.get_organisation())

            listing_entry = {
                "Name": linked_name,
                "Sortierschlüssel": expert.get_properties().get("Nachname", ""),
                "Organisation": organisation,
                "ORCID-Keywords": research_interest,
                "TaDiRAH-Zuordnung": expert.get_tadirah(formated=True),
                "Personenseite": f"{personal_page}"
                }

            entries.append(listing_entry)

        with open(os.path.join(path, filename), "w", encoding="utf-8") as f:
            yaml.dump(entries, f, allow_unicode=True, default_flow_style=False, sort_keys=False)

        logger.info(f"Das Expertbase-Objekt wurde erfolgreich zu einer YAML-Datei geparst und unter {path} gespeichert.")

    def add_properties_from_csv(self, path: str) -> None:
        """
        Mit dieser In-place Methode können die Eigenschaften der Experten in der Expertbase erweitert oder
        überschrieben werden.
        Die Eigenschaften werden in einer CSV-Datei nach dem folgenden Muster definiert:

        | name | orcid | neue_eigenschaft | neue_eigenschaft_2|(...)\n
        | (...) | (...) | wert|(...)|(...)\n
        (...)

        Args:
             path: Der Dateipfad zu der CSV-Datei.
        """

        try:
            csv_file = open(path, newline='', encoding='utf-8')
            reader = csv.reader(csv_file)
            properties = next(reader)
            orcids = self.get_orcids_as_list()
        except IOError as e:
            logger.error(f"Die Datei {path} konnte nicht geöffnet oder verarbeitet werden:\n {e}")
            raise

        if len(properties) < 2:
            logger.warning(f"Die CSV-Datei ist ungültig. Es muss mindestens 3 Spalten geben. Abbruch...")
            return

        elif not properties[0].lower() == "orcid":
            logger.warning(f"Die CSV-Datei ist ungültig. In der zweiten Spalte muss die ORCID stehen und die Spalte"
                           f" muss gültig benannt sein.")
            return

        for row in reader:

            current_orcid = row[0].lstrip('\ufeff').strip()

            if current_orcid in orcids:
                current_expert = self.base[current_orcid]
                for i, property in enumerate(properties[1:], 1):
                    new_properties = row[i]
                    if not row[i]:
                        continue
                    current_expert.extend_properties(property, new_properties)
                    self.raw_base[current_orcid][property] = new_properties
                    logger.info(f"Für den Experten {current_orcid} wurde die Eigenschaft '{property}'"
                                f" mit dem Wert '{new_properties}' angelegt oder überschrieben.")
            else:
                logger.warning(f"Der Experte {current_orcid} ist noch nicht Teil der Expertbase.")
