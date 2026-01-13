import os
import json
import logging
import time

import chevron
import requests

logger = logging.getLogger(__name__)

def search_wikidata_id(search_string: str, max_retries: int = 5) -> str:
    """
    Diese Funktion sucht die Wikidata-QID für eine Entität.

    Sie respektiert die Wikidata Robot Policy:
    - Setzt einen User-Agent Header mit Kontaktinfo
    - Handhabt 429-Rate-Limit-Fehler mit exponentiellem Backoff

    Args:
        search_string: Die Entität, nach der gesucht wird.
        max_retries: Anzahl der Wiederholungsversuche bei 429/Serverfehlern.
    Returns:
        Die QID oder der Suchstring, wenn kein Eintrag gefunden wird.
    """
    headers = {
        "User-Agent": "MyWikidataBot/1.0 (https://github.com/Nolram567/Expert-Base-Builder; mbgdevelopment@proton.me)"
    }

    params = {
        "action": "wbsearchentities",
        "language": "de",
        "format": "json",
        "search": search_string
    }

    retries = 0
    backoff = 1  # Sekunden

    while retries <= max_retries:
        try:
            response = requests.get(
                "https://www.wikidata.org/w/api.php",
                params=params,
                headers=headers,
                timeout=10
            )

            # Falls Rate-Limit
            if response.status_code == 429:
                logger.warning(f"HTTP-Statuscode 429 Too Many Requests – Warte {backoff} Sekunden...")
                time.sleep(backoff)
                retries += 1
                backoff = min(backoff * 2, 60)  # Max. 1 Minute warten
                continue

            response.raise_for_status()
            data = response.json()

            if data.get('search'):
                return data['search'][0]['id']
            else:
                logger.warning("Die Eingabe wurde nicht in Wikidata gefunden. Gebe Eingabe zurück...")
                return search_string

        except json.JSONDecodeError as e :
            logger.error(f"Die Antwort von Wikidata konnte nicht dekodiert werde: {e}.")
            return search_string
        except requests.RequestException as e:
            logger.error(f"Wikidata-Request fehlgeschlagen: {e}")
            return search_string

    logger.error("Maximale Anzahl an Retries erreicht.")
    return search_string

class Expert:
    """
    Objekte dieser Klasse repräsentieren einen Experten der HERMES-Expertbase.

    Diese Klasse stellt Methoden zur Verfügung, um einzelne Experten zu erstellen, zu verwalten und in semantische
    Repräsentationen zu übersetzen.

    Die Objektvariable "orcid" ist die ORCID des Experten.
    Die Objektvariable "properties" enthält alle definierten Eigenschaften des Experten als Dictionary nach dem Muster:
    {
        'Vorname': '(...)',
        'Nachname': '(...)',
        'Derzeitige Beschäftigung': [['(...)', '(...)', '(...)'], (...)],
        'Forschungsinteressen': ['(...)', '(...)', '(...)'],
        (...)
    }
    """

    tadirah_tooltips_path = None

    def __init__(self, orcid: str, data: dict):
        """
        Der Konstruktor der Klasse.

        Args:
            orcid: Die ORCID des Expertenobjekts.
            data: Die Eigenschaften des Expertenobjekts als Dictionary.
        """
        self.orcid = orcid
        self.properties = data

    def get_properties(self):
        """
        Diese Methode gibt die Eigenschaften des Expertenobjekts zurück.
        """
        return self.properties.copy()

    def get_orcid(self) -> str:
        """
        Diese Methode gibt die ORCID des Expertenobjekts zurück.
        """
        return self.orcid

    def get_name(self, formated: bool = True) -> str | tuple[str, str]:
        """
        Diese Methode gibt den Namen des Expertenobjekts zurück.

        Args:
            formated: Spezifiziert, ob der Name als Einzelstring formatiert zurückgegeben werden soll oder als Tupel mit
            dem Vor- und Nachnamen.
        Returns: Den Namen als Tupel aus vor uns Nachname oder als String nach dem Muster 'Vorname Nachname'.
        """
        return (
            f"{self.properties.get('Vorname', '')} {self.properties.get('Nachname', '')}"
            if formated
            else [
                self.properties.get("Vorname", ""),
                self.properties.get("Nachname", ""),
            ]
        )

    def get_current_employment(
        self, n, formated=True
    ) -> str | list[tuple[str, str, str]]:
        """
        Diese Methode gibt die derzeitige Beschäftigung als Liste von Tripeln zurück.

        Args:
            formated: Spezifiziert, ob die Rückgabe formatiert sein soll.
            n: Spezifiziert, wie viele Beschäftigungsverhältnisse maximal aufgenommen werden sollen.

        Returns:
            Die derzeitigen Beschäftigungsverhältnisse als Liste aus Tripeln mit Strings oder als formatierte Markdown-Aufzählung.
        """
        current_employment = self.properties.get("Derzeitige Beschäftigung", [])

        if formated:

            lines = []

            for i, entry in enumerate(current_employment):
                if i == n:
                    break

                parts = [part for part in entry if part]
                line = "* " + ", ".join(parts)
                lines.append(line)

            return "\n".join(lines)

        else:
            return self.properties.get("Derzeitige Beschäftigung", [])[:n]

    def get_mail(self) -> str:
        """
        Diese Methode gibt die E-Mail-Adresse des Experten als String zurück.

        Returns:
            Die E-Mail-Adresse oder einen leeren String.
        """
        return self.properties.get("E-Mail", "")

    def get_organisation(self) -> list[str]:
        """
        Die Methode gibt die Organisationen zurück, an denen der Experte derzeit beschäftigt ist. Sie nutzt die wikidata
        qid, um Duplikate in unterschiedlichen Schreibungen zu identifizieren.
        """
        current_employment = self.properties.get("Derzeitige Beschäftigung", [])
        organisations = []

        for employment in current_employment:
            if employment[2] not in organisations:
                organisations.append(employment[2])

        qids = {}

        for organisation in organisations:
            qid = search_wikidata_id(organisation)

            if qid not in qids.keys():
                qids[qid] = organisation

        return list(qids.values())

    def get_research_interest(self, formated=True) -> list[str] | str:
        """
        Die Methode gibt die Forschungsinteressen (ORCID-Keywords) des Experten zurück.

        Args:
            formated: Falls True, werden die Keywords zu einem String konkateniert und mit Semikola getrennt.

        Returns: Die ORCID-Keywords mit Semikola getrennt oder als Liste.
        """

        if formated:
            orcid_keywords = self.properties.get("Forschungsinteressen", [])

            if len(orcid_keywords) == 1 and "," in orcid_keywords[0]:
                orcid_keywords = [k.strip() for k in orcid_keywords[0].split(",")]

            # Ausgewählte Sonderzeichen entfernen, sodass der Filter korrekt generiert wird.
            for i, keyword in enumerate(orcid_keywords):
                if not keyword[0].isalnum():
                    orcid_keywords[i] = keyword.replace("(", "").replace(")", "").replace("#", "")

            orcid_keywords = [k.title() for k in orcid_keywords]

            return ";".join(orcid_keywords)
        else:
            return self.properties.get("Forschungsinteressen", [])

    def get_tadirah(self, formated=True) -> list[str] | str:
        """
        Diese Methode gibt die tadirah-Schlagwörter des Experten als Liste oder als string zurück.

        Args:
            formated: Wenn dieses Argument true ist, dann werden die Wörter konkateniert und mit Semikola getrennt.
        Returns:
            Die tadirah-Schlagwörter des Experten als String oder Liste von Strings.
        """
        if formated:
            tadirah = self.properties.get("TaDiRAH-Zuordnung", [])
            return ";".join(tadirah)
        else:
            return self.properties.get("TaDiRAH-Zuordnung", "")

    def extend_properties(self, property: str, value) -> None:
        """
        Die In-place Methode erweitert oder ersetzt die Eigenschaften des Expertenobjekts.

        Args:
            property: Der Name der Eigenschaft.
            value: Der Wert der Eigenschaft.
        """
        self.properties[property] = value

    def parse_qmd(self, output_directory_path: str, chevron_template_path: str) -> None:
        """
        Die Methode generiert auf der Grundlage des Expertenobjekts eine qmd-Seite für den HERMES Hub.

        Args:
            output_directory_path: Der relative Pfad zu dem Ordner für die Ausgabe des qmd-Dokuments.
            chevron_template_path: Der Pfad zum Chevron-Template, das für den Bau der Detailseiten verwendet werden soll.
        """

        logger.info(f"Das qmd-Dokument für {self.get_name()} wird erstellt...")

        with open(chevron_template_path, "r", encoding="utf-8") as qmd_template:
            template = qmd_template.read()

        formated_research_interest = Expert.__format_orcid_keywords(self.get_research_interest(formated=False))
        formated_tadirah = Expert.__format_tadirah_keywords(self.get_tadirah(formated=False))

        formated_template = chevron.render(
            template,
            {
                "expert-name": self.get_name(),
                "orcid-domain": f"https://orcid.org/{self.get_orcid()}",
                "current-employment": self.get_current_employment(n=3),
                "keywords": formated_research_interest,
                "tadirah": formated_tadirah,
                "e-mail": self.get_mail()
            },
        )

        output_path = os.path.join(output_directory_path,
                            f"{self.get_name(formated=False)[0].lower().strip().replace(" ", "-")}"
                            f"-"
                            f"{self.get_name(formated=False)[1].lower().strip().replace(" ", "-")}"
                            f".qmd")

        os.makedirs(output_directory_path, exist_ok=True)

        with open(output_path, "w", encoding="utf-8") as f:
            f.write(formated_template)

        logger.info(
            f"Das qmd-Dokument für {self.get_name()} wurde erstellt und unter {output_path} gespeichert..."
        )

    @staticmethod
    def __format_orcid_keywords(keywords: list[str]) -> str:
        """
        Die Helfermethode baut und formatiert das div-Element für die ORCID-Schlagwörter auf der Personenseite.

        Args:
            keywords: Die Keywords als Liste.
        Returns:
            Die ORCID Keywords als HTML Markup für die Personenseite.
        """
        if len(keywords) == 1 and "," in keywords[0]:
            keywords = [k.strip() for k in keywords[0].split(",")]

        builder = ['<div class="quarto-categories">']
        builder.extend(f'<span class="quarto-category tag-beige">{word}</span>' for word in keywords)
        builder.append("</div>")

        return "".join(builder)

    @staticmethod
    def __format_tooltip(keyword: str, tip: str) -> str:
        """
        Baut das abbr-Element für die tadirah-Schlagworte auf der Personenseite.
        """
        return f'<abbr data-tooltip="{tip}">{keyword}</abbr>'

    @staticmethod
    def __format_tadirah_keywords(keywords: list[str]) -> str:
        """
        Die Helfermethode baut und formatiert das div-Element für die tadirah-Schlagworte auf der Personenseite.

        Args:
            keywords: Die tadirah-Schlagworte als Liste von Strings.
        Returns:
            Die tadirah Keywords als HTML Markup für die Personenseite.
        """
        if len(keywords) == 1 and "," in keywords[0]:
            keywords = [k.strip() for k in keywords[0].split(",")]

        with open(Expert.tadirah_tooltips_path, 'r', encoding="utf-8") as file:
            tooltips = json.load(file)

        builder = ['<div class="quarto-categories">']
        builder.extend(f'<span class="quarto-category tag-tuerkis">{Expert.__format_tooltip(word, tooltips.get(word, ""))}</span>' for word in keywords)
        builder.append("</div>")

        return "".join(builder)
